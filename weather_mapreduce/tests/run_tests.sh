#!/bin/bash
# ============================================================
# Correctness tests for the Q8 weather MapReduce implementation.
#
#  * The hand-verified sample (19 output lines checked manually
#    in baseline) must match byte-for-byte for several mapper
#    counts, with and without the combiner, and with tiny mapper
#    flush intervals (forcing many partial blocks per mapper).
#  * Generated datasets (reproducible, 0.5 value grid) are checked
#    against the baseline sequential program — the same oracle
#    used for the MPI tests.
# ============================================================
set -u
cd "$(dirname "$0")/.."
ROOT="$(pwd)/.."   # repo root (contains weather_baselines/)
SAMPLE_DIR="$ROOT/weather_baselines/tests"
TMP=/tmp/opencode/q8mr_tests
mkdir -p "$TMP"
fail=0
pass=0

check() {
    local name="$1" input="$2" expected="$3" M="$4" combine="$5" flush="$6"
    local out="$TMP/out.txt"
    local opts=()
    [ "$combine" = "1" ] || opts+=(COMBINE=0)
    [ "$flush" != "-" ] && opts+=("Q8MR_FLUSH=$flush")
    if env "${opts[@]}" bash run_local.sh "$input" "$out" "$M" >/dev/null 2>&1 \
        && diff -q "$expected" "$out" >/dev/null; then
        echo "$name: PASS"; pass=$((pass+1))
    else
        echo "$name: FAIL"
        diff "$expected" "$out" 2>/dev/null | head -8
        fail=1
    fi
}

# ---------- hand-verified sample ----------
for M in 1 2 4 8; do
    check "sample_M$M" "$SAMPLE_DIR/sample.txt" "$SAMPLE_DIR/sample_expected.txt" "$M" 1 -
done
check "sample_M4_nocombiner" "$SAMPLE_DIR/sample.txt" "$SAMPLE_DIR/sample_expected.txt" 4 0 -
check "sample_M4_flush3" "$SAMPLE_DIR/sample.txt" "$SAMPLE_DIR/sample_expected.txt" 4 1 3

# ---------- generated datasets vs the sequential oracle ----------
i=0
for cfg in "--n 500 --k 3 --s 7 --seed 5" \
           "--n 2000 --k 5 --s 20 --seed 13" \
           "--n 10000 --k 10 --s 50 --seed 21" \
           "--n 1000 --k 5 --s 1 --seed 8" \
           "--n 999 --k 100 --s 6 --seed 99" \
           "--n 4000 --k 4 --s 300 --seed 77"; do
    i=$((i+1))
    python3 "$ROOT/weather_baselines/tools/gen_dataset.py" $cfg --out "$TMP/in_$i.txt"
    "$ROOT/weather_baselines/build/q8_sequential" "$TMP/in_$i.txt" > "$TMP/exp_$i.txt" \
        || { echo "sequential oracle failed on cfg $i"; fail=1; continue; }
    M=$(( (i % 4) + 1 ))
    check "gen_cfg$i(M=$M)" "$TMP/in_$i.txt" "$TMP/exp_$i.txt" "$M" 1 -
    check "gen_cfg${i}_nocombiner" "$TMP/in_$i.txt" "$TMP/exp_$i.txt" "$M" 0 -
    check "gen_cfg${i}_flush7" "$TMP/in_$i.txt" "$TMP/exp_$i.txt" "$M" 1 7
done

echo "----------------------------------------"
echo "$pass passed, $fail failed"
exit $fail
