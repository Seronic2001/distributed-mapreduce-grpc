#!/bin/bash
#SBATCH --job-name=q8_mr_weather
#SBATCH --output=q8mr_slurm_%j.out
#SBATCH --error=q8mr_slurm_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks=8
#SBATCH --cpus-per-task=1
#SBATCH --time=00:30:00
# (add --partition=<name> for your cluster; override any of the above on the
#  sbatch command line, e.g.: sbatch --ntasks=16 --nodes=2 run_slurm.sh ...)
# ============================================================
# run_slurm.sh — Slurm-based MapReduce execution for the Q8 weather
# analytics (Section 2 Q1), for use while the Hadoop environment on the cluster
# is unavailable.
#
# Role model: this script IS the MapReduce framework. The Slurm
# allocation is the "cluster": every MAP / COMBINE / REDUCE task runs as
# its own srun job step, scheduled across the allocation's nodes, while
# the script itself orchestrates
#
#   SPLIT -> MAP (srun) -> COMBINE (srun) -> SHUFFLE -> REDUCE (srun)
#
# with the exact same staging as run_local.sh (and Hadoop Streaming).
#
# Ways to run:
#   sbatch run_slurm.sh INPUT_FILE [OUT_FILE] [NUM_MAPPERS]
#   sbatch --ntasks=16 --nodes=2 run_slurm.sh INPUT_FILE ...
#   salloc --ntasks=8 bash run_slurm.sh INPUT_FILE ...
#   bash run_slurm.sh INPUT_FILE ...     # sequential fallback, no Slurm
#
# Env:
#   COMBINE=0      disable the combiner stage (default: enabled)
#   Q8MR_FLUSH     mapper flush interval in records (default 200000)
#   MR_TIMINGS     optional CSV to append stage timings to
#   MR_SEQ_STEPS=1 run tasks directly instead of via srun steps
#
# NOTE on scratch space: intermediate chunks/partials are created under
# the submission directory (shared FS), NOT /tmp — srun steps may land on
# other nodes whose /tmp is node-local.
# ============================================================
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

INPUT_RAW=${1:?usage: run_slurm.sh INPUT_FILE [OUT_FILE] [NUM_MAPPERS]}
OUT_RAW=${2:-q8mr_slurm_output.txt}
M=${3:-8}

[ -f "$INPUT_RAW" ] || { echo "input not found: $INPUT_RAW"; exit 1; }

INPUT="$(python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "$INPUT_RAW")"
OUT="$(python3 -c "import os, sys; print(os.path.abspath(sys.argv[1]))" "$OUT_RAW")"

cd "$SCRIPT_DIR"
COMBINE=${COMBINE:-1}

# Sibling tasks of a bare `srun bash run_slurm.sh ...` launch all run this
# script; only procid 0 orchestrates, the rest stand down.
if [ -n "${SLURM_PROCID:-}" ] && [ "${SLURM_PROCID}" != "0" ]; then
    exit 0
fi

# ---- task launcher -----------------------------------------------------
# Inside a Slurm session each task becomes an srun job step (scheduled on
# the allocation's nodes). If nested-srun restrictions block step creation,
# fall back to running the task locally rather than failing the job.
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

# ---- multi-task step layout (same helper as matmul_slurm.sh) -----------
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
echo "== run_slurm: $MODE — M=$M mappers, combiner=$COMBINE, host=$(hostname) =="

# ---- framework steps (identical staging to run_local.sh) ---------------
export Q8MR_FLUSH="${Q8MR_FLUSH:-200000}"   # exported: srun steps inherit it
TMP=$(mktemp -d "$PWD/.q8mr_slurm.XXXXXX")
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$(dirname "$OUT")" 2>/dev/null || true

# 1. SPLIT — line-aligned chunks, one per mapper task
split -d -a 3 -n "l/$M" "$INPUT" "$TMP/chunk_"
for i in $(seq 0 $((M - 1))); do
    c="$TMP/chunk_$(printf '%03d' "$i")"
    [ -f "$c" ] || : > "$c"
done

# 2. MAP — concurrent srun steps, distributed across the allocation
MAP_START=$(date +%s%N)
if [ "${MR_SINGLE_SRUN:-1}" = "1" ] && [ -n "${SLURM_JOB_ID:-}" ] && [ "${MR_SEQ_STEPS:-0}" != "1" ] && command -v srun >/dev/null 2>&1; then
    # shellcheck disable=SC2046  # step_layout's output is meant to split
    srun --ntasks="$M" $(step_layout "$M") --exact bash -c '
        i="$SLURM_PROCID"
        hostname > "'"$TMP"'/host_$i"
        c="'"$TMP"'/chunk_$(printf "%03d" "$i")"
        if [ -s "$c" ]; then
            ./build/q8_mapper < "$c" > "'"$TMP"'/map_$i.out"
        else
            : > "'"$TMP"'/map_$i.out"
        fi
    '
    report_placement "map"
else
    map_pids=()
    for i in $(seq 0 $((M - 1))); do
        c="$TMP/chunk_$(printf '%03d' "$i")"
        [ -s "$c" ] || continue
        launcher ./build/q8_mapper < "$c" > "$TMP/map_$i.out" &
        map_pids+=($!)
    done
    for pid in "${map_pids[@]}"; do
        wait "$pid"
    done
fi
MAP_END=$(date +%s%N)

# 3. COMBINE — map-side pre-merge, concurrent tasks across the allocation
COMBINE_START=$(date +%s%N)
if [ "$COMBINE" = "1" ]; then
    if [ "${MR_SINGLE_SRUN:-1}" = "1" ] && [ -n "${SLURM_JOB_ID:-}" ] && [ "${MR_SEQ_STEPS:-0}" != "1" ] && command -v srun >/dev/null 2>&1; then
        # shellcheck disable=SC2046
        srun --ntasks="$M" $(step_layout "$M") --exact bash -c '
            i="$SLURM_PROCID"
            f="'"$TMP"'/map_$i.out"
            if [ -s "$f" ]; then
                ./build/q8_combiner < "$f" > "'"$TMP"'/comb_$i.out"
            else
                : > "'"$TMP"'/comb_$i.out"
            fi
        '
    else
        comb_pids=()
        for i in $(seq 0 $((M - 1))); do
            [ -s "$TMP/map_$i.out" ] || continue
            launcher ./build/q8_combiner < "$TMP/map_$i.out" > "$TMP/comb_$i.out" &
            comb_pids+=($!)
        done
        for pid in "${comb_pids[@]}"; do
            wait "$pid"
        done
    fi
    cat "$TMP"/comb_*.out > "$TMP/shuffled"
else
    cat "$TMP"/map_*.out > "$TMP/shuffled"
fi
COMBINE_END=$(date +%s%N)

# 4+5. SHUFFLE+REDUCE — every merge is commutative, so ordering is irrelevant
REDUCE_START=$(date +%s%N)
launcher ./build/q8_reducer < "$TMP/shuffled" > "$OUT"
REDUCE_END=$(date +%s%N)

if [ -n "${MR_TIMINGS:-}" ]; then
    [ -f "$MR_TIMINGS" ] || echo "input,num_mappers,combiner,map_s,combine_s,reduce_s,total_s" > "$MR_TIMINGS"
    elapsed() { echo "scale=6; ($2 - $1) / 1000000000" | bc; }
    map_t=$(elapsed "$MAP_START" "$MAP_END")
    comb_t=$(elapsed "$COMBINE_START" "$COMBINE_END")
    red_t=$(elapsed "$REDUCE_START" "$REDUCE_END")
    tot_t=$(elapsed "$MAP_START" "$REDUCE_END")
    echo "$(basename "$INPUT"),$M,${COMBINE},$map_t,$comb_t,$red_t,$tot_t" >> "$MR_TIMINGS"
    echo "map=${map_t}s combine=${comb_t}s reduce=${red_t}s total=${tot_t}s (M=$M)" >&2
fi
echo "wrote $OUT"
