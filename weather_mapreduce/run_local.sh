#!/bin/bash
# ============================================================
# run_local.sh — local simulation of the Hadoop Streaming flow
# for the Q8 weather analytics (Section 2 Q1).
#
# Reproduces the framework steps on one machine:
#   1. SPLIT    : the dataset is split into M chunks (one per
#                 simulated mapper task), line-aligned.
#   2. MAP      : q8_mapper aggregates its chunk into partial
#                 blocks (flushing every Q8MR_FLUSH records).
#   3. COMBINE  : q8_combiner pre-merges the blocks on each
#                 "mapper node" (disable with COMBINE=0).
#   4. SHUFFLE  : cat + sort groups the pairs by key (all keys
#                 are global tags here, so one "reducer group").
#   5. REDUCE   : q8_reducer merges everything and prints the
#                 19-line report — identical to the sequential
#                 and MPI implementations of baseline.
#
# Usage:
#   bash run_local.sh INPUT_FILE [OUT_FILE] [NUM_MAPPERS]
# Env:
#   COMBINE=0      disable the combiner stage (default: enabled)
#   Q8MR_FLUSH     mapper flush interval in records (default 200000)
#   MR_TIMINGS     optional file to append stage timings to (CSV)
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

INPUT=${1:?usage: run_local.sh INPUT_FILE [OUT_FILE] [NUM_MAPPERS]}
OUT=${2:-/tmp/opencode/q8mr_output.txt}
M=${3:-4}
[ -f "$INPUT" ] || { echo "input not found: $INPUT"; exit 1; }
mkdir -p "$(dirname "$OUT")"

TMP=$(mktemp -d /tmp/q8mr.XXXXXX)
trap 'rm -rf "$TMP"' EXIT
COMBINE=${COMBINE:-1}

# ---- 1. SPLIT (setup, not measured — Hadoop splits happen at job submission) ----
split -d -a 3 -n "l/$M" "$INPUT" "$TMP/chunk_"
for i in $(seq 0 $((M - 1))); do
    c="$TMP/chunk_$(printf '%03d' "$i")"
    [ -f "$c" ] || : > "$c"
done

# ---- 2. MAP ----
MAP_START=$(date +%s%N)
map_pids=()
for i in $(seq 0 $((M - 1))); do
    c="$TMP/chunk_$(printf '%03d' "$i")"
    [ -s "$c" ] || continue
    Q8MR_FLUSH="${Q8MR_FLUSH:-200000}" ./build/q8_mapper < "$c" > "$TMP/map_$i.out" &
    map_pids+=($!)
done
for pid in "${map_pids[@]}"; do
    wait "$pid"
done
MAP_END=$(date +%s%N)

# ---- 3. COMBINE (per mapper node) ----
COMBINE_START=$(date +%s%N)
if [ "$COMBINE" = "1" ]; then
    comb_pids=()
    for i in $(seq 0 $((M - 1))); do
        [ -s "$TMP/map_$i.out" ] || continue
        ./build/q8_combiner < "$TMP/map_$i.out" > "$TMP/comb_$i.out" &
        comb_pids+=($!)
    done
    for pid in "${comb_pids[@]}"; do
        wait "$pid"
    done
    cat "$TMP"/comb_*.out > "$TMP/shuffled"
else
    cat "$TMP"/map_*.out > "$TMP/shuffled"
fi
COMBINE_END=$(date +%s%N)

# ---- 4+5. SHUFFLE (order is irrelevant — every merge is commutative) + REDUCE ----
REDUCE_START=$(date +%s%N)
./build/q8_reducer < "$TMP/shuffled" > "$OUT"
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
