#!/bin/bash
# ============================================================
# run_local.sh — local simulation of the Hadoop MapReduce flow
# for the Row-Row matrix multiplication (Section 1, Q1).
#
# Reproduces the framework steps on one machine:
#   1. SPLIT      : matrix A (the distributed input) is split into R
#                   chunks, one per simulated mapper task.
#   2. REPLICATE  : matrix B is copied into a per-mapper "distributed
#                   cache" directory (full replication on every node).
#   3. MAP        : mapper.py processes its chunk of A independently,
#                   reading the replicated B from the local cache copy.
#   4. SHUFFLE    : sort groups all pairs with the same key.
#   5. COMBINE    : combiner.py pre-sums partial products per mapper
#                   (set COMBINE=0 to disable).
#   6. SHUFFLE    : sort again after combining.
#   7. REDUCE     : reducer.py sums the terms and reconstructs C.
#
# Usage:
#   bash run_local.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]
# Env:
#   COMBINE=0        disable the combiner stage (default: enabled)
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

A_FILE=${1:?usage: run_local.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]}
B_FILE=${2:?usage: run_local.sh A_FILE B_FILE [OUT_FILE] [NUM_MAPPERS]}
OUT_FILE=${3:-C_output.txt}
R=${4:-3}
[ -f "$A_FILE" ] || { echo "A file not found: $A_FILE"; exit 1; }
[ -f "$B_FILE" ] || { echo "B file not found: $B_FILE"; exit 1; }

TMP=$(mktemp -d /tmp/mr_matmul.XXXXXX)
trap 'rm -rf "$TMP"' EXIT

# Matrix dimensions (validated): A is m x n, B is n x p.
# is_sparse_a: sparse ijv A files carry explicit row ids on every line;
# dense row files rely on the split offset, exactly as a Hadoop
# InputFormat would supply record offsets to each mapper.
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
echo "C = A x B : A ${M}x${N}, B ${N}x${P}, $R mappers" >&2

# ---- 1. SPLIT: distribute the rows of A over R mapper tasks ----
split -d -a 3 -n "l/$R" "$A_FILE" "$TMP/chunk_"
chunks=()
for r in $(seq 0 $((R - 1))); do
    c="$TMP/chunk_$(printf '%03d' "$r")"
    [ -f "$c" ] || touch "$c"
    chunks+=("$c")
done

# ---- 2. REPLICATE: simulate the distributed cache (B on every node) ----
for r in $(seq 0 $((R - 1))); do
    mkdir -p "$TMP/cache_$r"
    cp "$B_FILE" "$TMP/cache_$r/"
done

# ---- 3. MAP: each mapper works only on its own chunk + local B copy ----
# For dense A the runner supplies each mapper the global row offset of its
# chunk (the role a Hadoop InputFormat plays for line-based records).
offsets=()
total=0
for r in $(seq 0 $((R - 1))); do
    offsets+=("$total")
    c="$TMP/chunk_$(printf '%03d' "$r")"
    if [ -f "$c" ]; then
        total=$((total + $(wc -l < "$c")))
    fi
done

MAP_START=$(date +%s%N)
map_pids=()
for r in $(seq 0 $((R - 1))); do
    chunk="$TMP/chunk_$(printf '%03d' "$r")"
    [ -f "$chunk" ] || touch "$chunk"
    offset_args=()
    if [ "$IS_SPARSE_A" = "0" ]; then
        offset_args=("--row-offset=${offsets[$r]}")
    fi
    python3 mapper.py "${offset_args[@]}" "$TMP/cache_$r/$(basename "$B_FILE")" \
        < "$chunk" > "$TMP/map_$r.out" &
    map_pids+=($!)
done
for pid in "${map_pids[@]}"; do
    wait "$pid"
done
MAP_END=$(date +%s%N)

# ---- 4. SHUFFLE: group intermediate pairs by key ----
SHUFFLE_START=$(date +%s%N)
cat "$TMP"/map_*.out | sort > "$TMP/shuffled1"
SHUFFLE_END=$(date +%s%N)

# ---- 5+6. COMBINE + SHUFFLE (combiner is a local mini-reducer) ----
COMBINE_START=$(date +%s%N)
if [ "${COMBINE:-1}" = "1" ]; then
    python3 combiner.py < "$TMP/shuffled1" | sort > "$TMP/shuffled2"
else
    cp "$TMP/shuffled1" "$TMP/shuffled2"
fi
COMBINE_END=$(date +%s%N)

# ---- 7. REDUCE: reconstruct C (header carries output dimensions; only
# emitted for dense A, so sparse-A jobs return sparse C like the oracle) ----
REDUCE_START=$(date +%s%N)
if [ "$IS_SPARSE_A" = "1" ]; then
    cat "$TMP/shuffled2" | python3 reducer.py > "$OUT_FILE"
else
    { echo "C $M $N $P"; cat "$TMP/shuffled2"; } | python3 reducer.py > "$OUT_FILE"
fi
REDUCE_END=$(date +%s%N)

elapsed() { echo "scale=3; ($2 - $1) / 1000000000" | bc; }
echo "map=$(elapsed $MAP_START $MAP_END)s shuffle=$(elapsed $SHUFFLE_START $SHUFFLE_END)s combine=$(elapsed $COMBINE_START $COMBINE_END)s reduce=$(elapsed $REDUCE_START $REDUCE_END)s" >&2
echo "wrote $OUT_FILE"
