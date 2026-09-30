#!/bin/bash
#SBATCH --job-name=mr_matmul
#SBATCH --output=matmul_slurm_%j.out
#SBATCH --error=matmul_slurm_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=4
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
# (add --partition=<name> for your cluster; override on the sbatch command
#  line, e.g.: sbatch --ntasks=8 --nodes=2 matmul_slurm.sh A.txt B.txt C.txt 8)
# ============================================================
# matmul_slurm.sh — Slurm-based MapReduce execution of the Row-Row
# matrix multiplication (Section 1 Q1), for use while the Hadoop
# environment on the cluster is unavailable.
#
#   SPLIT A -> REPLICATE B (cache dirs) -> MAP (srun)
#        -> SHUFFLE -> COMBINE (srun) -> SHUFFLE -> REDUCE (srun)
#
# with the exact same staging as run_local.sh; the Slurm allocation is
# the "cluster" and every task is an srun job step.
#
# Ways to run:
#   sbatch matmul_slurm.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]
#   salloc --ntasks=4 bash matmul_slurm.sh A_FILE B_FILE ...
#   bash matmul_slurm.sh A_FILE B_FILE ...      # sequential fallback
#
# Env:
#   COMBINE=0   disable the combiner stage (default: enabled)
# ============================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

A_RAW=${1:?usage: matmul_slurm.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]}
B_RAW=${2:?usage: matmul_slurm.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]}
OUT_RAW=${3:-C_slurm_output.txt}
R=${4:-4}

[ -f "$A_RAW" ] || { echo "A file not found: $A_RAW"; exit 1; }
[ -f "$B_RAW" ] || { echo "B file not found: $B_RAW"; exit 1; }

A_FILE="$(python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "$A_RAW")"
B_FILE="$(python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "$B_RAW")"
OUT_FILE="$(python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "$OUT_RAW")"

cd "$SCRIPT_DIR"
COMBINE=${COMBINE:-1}

# Only procid 0 orchestrates in a bare `srun bash matmul_slurm.sh` launch.
if [ -n "${SLURM_PROCID:-}" ] && [ "${SLURM_PROCID}" != "0" ]; then
    exit 0
fi

# ---- task launcher (same policy as weather_mapreduce/run_slurm.sh) -
if [ -n "${SLURM_JOB_ID:-}" ] && [ "${MR_SEQ_STEPS:-0}" != "1" ] \
        && command -v srun >/dev/null 2>&1; then
    launcher() {
        if ! srun --ntasks=1 --nodes=1 --exact --chdir="$PWD" "$@"; then
            echo "warn: srun step refused — running task locally: $1" >&2
            "$@"
        fi
    }
    MODE="slurm job ${SLURM_JOB_ID}"
else
    launcher() { "$@"; }
    MODE="sequential fallback (no Slurm allocation)"
fi

# ---- multi-task step layout (same helper as run_slurm.sh) --------------
# Without an explicit node count srun packs a step onto as few nodes as it
# can, and the job's inherited --ntasks-per-node is ignored with a warning,
# so every task lands on the first node. Spread TASKS evenly over the
# allocation instead (one node per task at most), and allow more than one
# task per CPU only when the step has more tasks than the job.
step_layout() {
    local tasks=$1
    local nodes=${SLURM_JOB_NUM_NODES:-${SLURM_NNODES:-1}}
    [ "$nodes" -gt "$tasks" ] && nodes=$tasks
    local per_node=$(( (tasks + nodes - 1) / nodes ))
    local args="--nodes=$nodes --ntasks-per-node=$per_node --distribution=block"
    [ "$tasks" -gt "${SLURM_NTASKS:-$tasks}" ] && args="$args --overcommit"
    echo "$args"
}

# Print how many tasks of a step ran on each host (from $TMP/host_*).
report_placement() {
    echo "   $1 placement: $(cat "$TMP"/host_* 2>/dev/null | sort | uniq -c \
        | awk '{printf "%s%s x%s", sep, $2, $1; sep=", "}')"
    rm -f "$TMP"/host_*
}

# ---- matrix dimensions (identical validation to run_local.sh) ----------
read -r M N P IS_SPARSE_A < <(python3 - "$A_FILE" "$B_FILE" <<'PY'
import sys

def dims(path):
    first, dense, nrows, ncols = True, False, 0, 0
    with open(path) as stream:
        for raw in stream:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            if first:
                dense = "\t" not in line
                first = False
            if dense:
                if nrows == 0:
                    ncols = len(line.split())
                nrows += 1
            else:
                i, j, _ = line.split("\t")
                nrows = max(nrows, int(i) + 1)
                ncols = max(ncols, int(j) + 1)
    return nrows, ncols, (not dense)

am, an, sparse_a = dims(sys.argv[1])
bn, bp, _ = dims(sys.argv[2])
if an != bn:
    sys.exit("dimension mismatch: A is %dx%d, B is %dx%d" % (am, an, bn, bp))
print(am, an, bp, 1 if sparse_a else 0)
PY
)
echo "== matmul_slurm: $MODE — C = A(${M}x${N}) x B(${N}x${P}), R=$R mappers =="

TMP=$(mktemp -d "$PWD/.mrmatmul_slurm.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$(dirname "$OUT_FILE")" 2>/dev/null || true

# ---- 1. SPLIT A into R row-aligned chunks ------------------------------
split -d -a 3 -n "l/$R" "$A_FILE" "$TMP/chunk_"
chunks=()
for r in $(seq 0 $((R - 1))); do
    c="$TMP/chunk_$(printf '%03d' "$r")"
    [ -f "$c" ] || touch "$c"
    chunks+=("$c")
done

# ---- 2. REPLICATE B into per-mapper cache dirs (distributed cache) -----
for r in $(seq 0 $((R - 1))); do
    mkdir -p "$TMP/cache_$r"
    cp "$B_FILE" "$TMP/cache_$r/"
done

# ---- 3. MAP — concurrent srun steps, one per chunk of A ----------------
# Dense A needs each chunk's global row offset (the InputFormat emulation).
offsets=()
total=0
for r in $(seq 0 $((R - 1))); do
    offsets+=("$total")
    c="$TMP/chunk_$(printf '%03d' "$r")"
    if [ -f "$c" ]; then
        total=$((total + $(wc -l < "$c")))
    fi
done
printf "%s\n" "${offsets[@]}" > "$TMP/offsets.txt"

MAP_START=$(date +%s%N)
if [ "${MR_SINGLE_SRUN:-1}" = "1" ] && [ -n "${SLURM_JOB_ID:-}" ] && [ "${MR_SEQ_STEPS:-0}" != "1" ] && command -v srun >/dev/null 2>&1; then
    # shellcheck disable=SC2046  # step_layout's output is meant to split
    srun --ntasks="$R" $(step_layout "$R") --exact bash -c '
        r="$SLURM_PROCID"
        hostname > "'"$TMP"'/host_$r"
        chunk="'"$TMP"'/chunk_$(printf "%03d" "$r")"
        cache="'"$TMP"'/cache_$r/'"$(basename "$B_FILE")"'"
        offset_args=()
        if [ "'"$IS_SPARSE_A"'" = "0" ]; then
            offset=$(sed -n "$((r + 1))p" "'"$TMP"'/offsets.txt")
            offset_args=(--row-offset="$offset")
        fi
        python3 mapper.py "${offset_args[@]}" "$cache" < "$chunk" > "'"$TMP"'/map_$r.out"
    '
    report_placement "map"
else
    map_pids=()
    for r in $(seq 0 $((R - 1))); do
        chunk="$TMP/chunk_$(printf '%03d' "$r")"
        offset_args=()
        if [ "$IS_SPARSE_A" = "0" ]; then
            offset_args=(--row-offset="${offsets[$r]}")
        fi
        launcher python3 mapper.py "${offset_args[@]}" \
            "$TMP/cache_$r/$(basename "$B_FILE")" < "$chunk" > "$TMP/map_$r.out" &
        map_pids+=($!)
    done
    for pid in "${map_pids[@]}"; do
        wait "$pid"
    done
fi
MAP_END=$(date +%s%N)

# ---- 4. SHUFFLE: group intermediate pairs by key -----------------------
SHUFFLE_START=$(date +%s%N)
cat "$TMP"/map_*.out | sort > "$TMP/shuffled1"
SHUFFLE_END=$(date +%s%N)

# ---- 5+6. COMBINE + SHUFFLE (combiner is a local mini-reducer) ---------
COMBINE_START=$(date +%s%N)
if [ "$COMBINE" = "1" ]; then
    launcher python3 combiner.py < "$TMP/shuffled1" | sort > "$TMP/shuffled2"
else
    cp "$TMP/shuffled1" "$TMP/shuffled2"
fi
COMBINE_END=$(date +%s%N)

# ---- 7. REDUCE: reconstruct C (header carries output dimensions; only
# emitted for dense A, so sparse-A jobs return sparse C like the oracle) --
REDUCE_START=$(date +%s%N)
if [ "$IS_SPARSE_A" = "1" ]; then
    launcher python3 reducer.py < "$TMP/shuffled2" > "$OUT_FILE"
else
    { echo "C $M $N $P"; cat "$TMP/shuffled2"; } \
        | launcher python3 reducer.py > "$OUT_FILE"
fi
REDUCE_END=$(date +%s%N)

if [ -n "${MR_TIMINGS:-}" ]; then
    [ -f "$MR_TIMINGS" ] || echo "input,num_mappers,combiner,map_s,shuffle1_s,combine_s,reduce_s,total_s" > "$MR_TIMINGS"
    elapsed() { echo "scale=6; ($2 - $1) / 1000000000" | bc; }
    echo "$(basename "$A_FILE"),$R,${COMBINE},$(elapsed "$MAP_START" "$MAP_END"),$(elapsed "$SHUFFLE_START" "$SHUFFLE_END"),$(elapsed "$COMBINE_START" "$COMBINE_END"),$(elapsed "$REDUCE_START" "$REDUCE_END"),$(elapsed "$MAP_START" "$REDUCE_END")" >> "$MR_TIMINGS"
fi
echo "wrote $OUT_FILE"
