#!/bin/bash
# ============================================================
# benchmark.sh — MPI vs MapReduce comparison for the Q8 weather
# analytics (Section 2 Q1).
#
# Local mode (default, no Hadoop required):
#   Runs the baseline sequential program, the baseline MPI
#   program (P=1,2,4,8) and the MapReduce pipeline simulation
#   (run_local.sh, M mappers with combiner) over the same
#   reproducible datasets. Writes:
#     results/comparison.csv        all timings, one row per run
#     results/summary.md            rendered table (median of reps)
#
# Hadoop mode (HADOOP=1, needs HADOOP_HOME + a running YARN):
#   Additionally runs the real Streaming job via run_hadoop.sh
#   and appends those rows to comparison.csv too.
#
# Env overrides:
#   SIZES="10000 100000 1000000"  dataset sizes (records)
#   MAPPERS="1 2 4 8"             mapper counts for the MR pipeline
#   MPS="1 2 4 8"                 MPI process counts
#   REPS=3                        repetitions (median is reported)
#   HADOOP=1                      include real Hadoop Streaming runs
# ============================================================
set -u
cd "$(dirname "$0")"
ROOT="$(cd .. && pwd)"                      # repo root (has weather_baselines/)
RESULTS=results
mkdir -p "$RESULTS" /tmp/opencode
CSV="$RESULTS/comparison.csv"
echo "input,records,engine,workers,map_s,combine_s,reduce_s,total_s" > "$CSV"

SIZES=${SIZES:-"10000 100000 1000000"}
if [ -n "${SLURM_NTASKS:-}" ] && [ "${SLURM_NTASKS}" -ge 16 ]; then
    DEFAULT_WORKERS="1 2 4 8 16"
else
    DEFAULT_WORKERS="1 2 4 8"
fi
MAPPERS=${MAPPERS:-$DEFAULT_WORKERS}
MPS=${MPS:-$DEFAULT_WORKERS}
REPS=${REPS:-3}
STATIONS=${STATIONS:-500}
K=${K:-10}
SEED=${SEED:-42}

# ---- build everything ----
make -s
g++ -O2 -std=c++17 -o "$ROOT/weather_baselines/build/q8_sequential" "$ROOT/weather_baselines/src/q8_sequential.cpp"

MPI_EXEC="mpiexec"
HAVE_MPI=1
if ! command -v mpicxx >/dev/null 2>&1 || ! command -v mpiexec >/dev/null 2>&1; then
    HAVE_MPI=0
    echo "WARNING: mpicxx/mpiexec not found — MPI rows will be skipped." \
         "Run this script on the cluster (where baseline ran) for MPI timings."
fi
if [ "$HAVE_MPI" = "1" ] && mpiexec --help 2>&1 | grep -q -- '--oversubscribe'; then
    MPI_EXEC="mpiexec --oversubscribe"
fi

median3() { sort -g | awk 'NR==2 {print; exit}'; }
now_ns() { date +%s%N; }

for N in $SIZES; do
    f=/tmp/opencode/q8_bench_$N.txt
    [ -f "$f" ] || python3 "$ROOT/weather_baselines/tools/gen_dataset.py" \
        --n "$N" --k "$K" --s "$STATIONS" --seed "$SEED" --out "$f"
    echo "=== N=$N ==="

    # ---- sequential baseline ----
    for r in 1 2 3; do
        t0=$(now_ns)
        "$ROOT/weather_baselines/build/q8_sequential" "$f" >/dev/null 2>/dev/null
        t1=$(now_ns)
        eval "T$r=$(awk -v a="$t0" -v b="$t1" 'BEGIN{print (b-a)/1e9}')"
    done
    seq_t=$(median3 < <(printf '%s\n%s\n%s\n' "$T1" "$T2" "$T3"))
    echo "$N,$N,sequential,1,0,0,0,$seq_t" >> "$CSV"
    echo "  seq    : ${seq_t}s"

    # ---- MPI ----
    if [ "$HAVE_MPI" = "1" ]; then
        for P in $MPS; do
            for r in 1 2 3; do
                "$MPI_EXEC" -n "$P" "$ROOT/weather_baselines/build/q8_mpi" "$f" \
                    >/dev/null 2>/tmp/opencode/mpit.txt
                eval "T$r=$(grep '^TIME_TOTAL' /tmp/opencode/mpit.txt | awk '{print $2}')"
            done
            mpi_t=$(median3 < <(printf '%s\n%s\n%s\n' "$T1" "$T2" "$T3"))
            echo "$N,$N,mpi,$P,0,0,0,$mpi_t" >> "$CSV"
            echo "  mpi P=$P: ${mpi_t}s"
        done
    fi

    # ---- MapReduce pipeline (run_slurm.sh on cluster, run_local.sh locally) ----
    MR_RUNNER="run_local.sh"
    if [ -n "${SLURM_JOB_ID:-}" ] && [ -f "run_slurm.sh" ]; then
        MR_RUNNER="run_slurm.sh"
    fi
    for M in $MAPPERS; do
        rm -f /tmp/opencode/mr_t.csv
        for r in 1 2 3; do
            MR_TIMINGS=/tmp/opencode/mr_t.csv \
                bash "$MR_RUNNER" "$f" /tmp/opencode/q8mr_bench_out.txt "$M" 2>/dev/null
        done
        # median of the 3 total_s columns (7th field)
        total_t=$(tail -3 /tmp/opencode/mr_t.csv | cut -d, -f7 | sort -g | sed -n 2p)
        map_t=$(tail -3 /tmp/opencode/mr_t.csv | cut -d, -f4 | sort -g | sed -n 2p)
        combine_t=$(tail -3 /tmp/opencode/mr_t.csv | cut -d, -f5 | sort -g | sed -n 2p)
        reduce_t=$(tail -3 /tmp/opencode/mr_t.csv | cut -d, -f6 | sort -g | sed -n 2p)
        echo "$N,$N,mapreduce,$M,$map_t,$combine_t,$reduce_t,$total_t" >> "$CSV"
        echo "  mr  M=$M: ${total_t}s"
    done

    # ---- optional: real Hadoop Streaming ----
    if [ "${HADOOP:-0}" = "1" ]; then
        if command -v hadoop >/dev/null 2>&1 || [ -n "${HADOOP_HOME:-}" ]; then
            t0=$(now_ns)
            if bash run_hadoop.sh "$f" /tmp/opencode/q8mr_hadoop.txt >/dev/null 2>&1; then
                t1=$(now_ns)
                hadoop_t=$(awk -v a="$t0" -v b="$t1" 'BEGIN{print (b-a)/1e9}')
                echo "$N,$N,hadoop-streaming,-,0,0,0,$hadoop_t" >> "$CSV"
                echo "  hadoop : ${hadoop_t}s"
            else
                echo "  hadoop : run failed (check HADOOP_HOME/YARN)"
            fi
        else
            echo "  hadoop : skipped (hadoop/HADOOP_HOME not available)"
        fi
    fi
done

# ---- summary ----
python3 - "$CSV" > "$RESULTS/summary.md" <<'PY'
import csv, sys
from collections import defaultdict

rows = list(csv.DictReader(open(sys.argv[1])))
by_size = defaultdict(list)
for row in rows:
    by_size[row["records"]].append(row)

print("# Q8 weather: MPI vs MapReduce — timing summary\n")
print("Times are medians of 3 runs (seconds). "
      "MR stage columns are map/combine/reduce.\n")
for records, rs in sorted(by_size.items(), key=lambda kv: int(kv[0])):
    print(f"## N = {records} records\n")
    print("| engine | workers | map | combine | reduce | total |")
    print("|---|---|---|---|---|---|")
    for row in rs:
        print("| {engine} | {workers} | {map_s} | {combine_s} | {reduce_s} | **{total_s}** |".format(**row))
    print()
PY
echo "wrote $CSV and $RESULTS/summary.md"
