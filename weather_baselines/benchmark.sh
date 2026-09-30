#!/bin/bash
# Benchmark Q8 weather analytics: sequential vs MPI (P=1,2,4,8) over increasing sizes.
# Usage: bash benchmark.sh [outdir]
set -e
cd "$(dirname "$0")"
BIN=build
mkdir -p "$BIN" "${1:-results}" /tmp/opencode
mpicxx -O2 -std=c++17 -o "$BIN/q8_mpi" src/q8_mpi.cpp
g++ -O2 -std=c++17 -o "$BIN/q8_sequential" src/q8_sequential.cpp
CSV="${1:-results}/benchmark.csv"
echo "input,P,total,compute,distribute,collect" > "$CSV"

MPI_EXEC="mpiexec"
if mpiexec --help 2>&1 | grep -q -- '--oversubscribe'; then
    MPI_EXEC="mpiexec --oversubscribe"
fi

SIZES=${SIZES:-"10000 100000 1000000"}
K=${K:-10}
STATIONS=${STATIONS:-500}
SEED=${SEED:-42}
REPS=${REPS:-3}

for N in $SIZES; do
    name="N${N}_S${STATIONS}"
    f=/tmp/opencode/q8_bench_${N}.txt
    [ -f "$f" ] || python3 tools/gen_dataset.py --n "$N" --k "$K" --s "$STATIONS" --seed "$SEED" --out "$f"
    echo "=== Q8 $name ==="
    for spec in seq 1 2 4 8; do
        best=""
        bdist="0.0"; bcomp="0.0"; bcoll="0.0"
        for rep in $(seq 1 $REPS); do
            if [ "$spec" = "seq" ]; then
                /usr/bin/time -f "%e" -o /tmp/opencode/q8_wall.txt \
                    "$BIN/q8_sequential" "$f" > /dev/null 2>/dev/null || true
                t=$(cat /tmp/opencode/q8_wall.txt 2>/dev/null)
                d=0; c="$t"; g=0
            else
                P=$spec
                if [ "$P" -gt 1 ]; then RUN=($MPI_EXEC -n "$P"); else RUN=($MPI_EXEC -n 1); fi
                "${RUN[@]}" "$BIN/q8_mpi" "$f" > /dev/null 2> /tmp/opencode/q8_time.txt || true
                t=$(grep '^TIME_TOTAL' /tmp/opencode/q8_time.txt 2>/dev/null | awk '{print $2}')
                c=$(grep '^TIME_COMPUTE' /tmp/opencode/q8_time.txt 2>/dev/null | awk '{print $2}')
                d=$(grep '^TIME_DISTRIBUTE' /tmp/opencode/q8_time.txt 2>/dev/null | awk '{print $2}')
                g=$(grep '^TIME_COLLECT' /tmp/opencode/q8_time.txt 2>/dev/null | awk '{print $2}')
            fi
            if [ -n "$t" ]; then
                better=$(python3 -c "import sys; a,b=float('$best' or 1e18),float('$t'); print(1 if b<a else 0)" 2>/dev/null || echo 0)
                if [ "$better" = "1" ] || [ -z "$best" ]; then
                    best="$t"; bdist="$d"; bcomp="$c"; bcoll="$g"
                fi
            fi
        done
        label=$([ "$spec" = "seq" ] && echo "sequential" || echo "$spec")
        if [ -n "$best" ]; then
            echo "$name,$label,$best,$bcomp,$bdist,$bcoll" >> "$CSV"
            echo "$label total=$best compute=$bcomp"
        else
            echo "$label run failed"
        fi
    done
done
echo "results written to $CSV"
