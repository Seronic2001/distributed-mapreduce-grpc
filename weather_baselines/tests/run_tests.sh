#!/bin/bash
set -e
cd "$(dirname "$0")/.."
SRC=src
BIN=build
mkdir -p "$BIN" /tmp/opencode
mpicxx -O2 -std=c++17 -o "$BIN/q8_mpi" "$SRC/q8_mpi.cpp"
g++ -O2 -std=c++17 -o "$BIN/q8_sequential" "$SRC/q8_sequential.cpp"

MPI_EXEC="mpiexec"
if mpiexec --help 2>&1 | grep -q -- '--oversubscribe'; then
    MPI_EXEC="mpiexec --oversubscribe"
fi

fail=0
check() {
    local name="$1" input="$2" expected="$3" P="$4"
    if [ "$P" = "seq" ]; then
        "$BIN/q8_sequential" "$input" > /tmp/opencode/q8_out.txt 2>/tmp/opencode/q8_err.txt || { echo "$name P=seq: RUN FAIL"; cat /tmp/opencode/q8_err.txt; fail=1; return; }
    elif [ "$P" -gt 1 ]; then
        $MPI_EXEC -n "$P" "$BIN/q8_mpi" "$input" > /tmp/opencode/q8_out.txt 2>/tmp/opencode/q8_err.txt || { echo "$name P=$P: RUN FAIL"; cat /tmp/opencode/q8_err.txt; fail=1; return; }
    else
        "$BIN/q8_mpi" "$input" > /tmp/opencode/q8_out.txt 2>/tmp/opencode/q8_err.txt || { echo "$name P=$P: RUN FAIL"; cat /tmp/opencode/q8_err.txt; fail=1; return; }
    fi
    if diff -q "$expected" /tmp/opencode/q8_out.txt > /dev/null; then
        echo "$name P=$P: PASS"
    else
        echo "$name P=$P: FAIL"
        diff "$expected" /tmp/opencode/q8_out.txt | head -10
        fail=1
    fi
}

check sample tests/sample.txt tests/sample_expected.txt seq
for P in 1 2 4 8; do check sample tests/sample.txt tests/sample_expected.txt $P; done

i=0
for cfg in "--n 500 --k 3 --s 7 --seed 5" \
           "--n 2000 --k 5 --s 20 --seed 13" \
           "--n 10000 --k 10 --s 50 --seed 21" \
           "--n 1000 --k 5 --s 1 --seed 8" \
           "--n 999 --k 100 --s 6 --seed 99" \
           "--n 4000 --k 4 --s 300 --seed 77"; do
    i=$((i+1))
    python3 tools/gen_dataset.py $cfg --out /tmp/opencode/q8_rand_$i.txt
    "$BIN/q8_sequential" /tmp/opencode/q8_rand_$i.txt > /tmp/opencode/q8_ref_$i.txt
    for P in 1 2 4 8; do check "random_cfg$i" /tmp/opencode/q8_rand_$i.txt /tmp/opencode/q8_ref_$i.txt $P; done
done

exit $fail
