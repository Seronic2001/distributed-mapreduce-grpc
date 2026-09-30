#!/bin/bash
# ============================================================
# run_hadoop.sh — Q8 weather analytics on Hadoop Streaming
# (Hadoop 3.3.6 + YARN, C++ mapper/combiner/reducer binaries).
#
# Usage:
#   bash run_hadoop.sh DATASET.TXT [OUTPUT_REPORT] [NUM_MAPPERS_HINT]
#
# Requirements:
#   * HADOOP_HOME set or `hadoop` on PATH (tested against 3.3.6)
#   * Binaries built:  make            (see Makefile)
#     The binaries are shipped to the cluster nodes via -files.
#
# Design (see README.md for the full discussion):
#   1. The dataset is uploaded to HDFS — the distributed input.
#   2. Hadoop splits it and streams each split to one map task;
#      every task runs ./q8_mapper, which aggregates its records
#      into partial blocks (no inter-task communication).
#   3. The combiner (./q8_combiner) pre-merges the blocks on each
#      node so per-record data never crosses the network.
#   4. ONE reducer (./q8_reducer) receives the pre-aggregated
#      partials, merges them, and prints the 19-line report —
#      byte-identical to the baseline sequential/MPI output.
#      (A single global report requires a single reducer: every
#      wire-format key is a global tag, so a multi-reducer job
#      would print one partial report per reducer.)
#   5. The report is copied back from the job output directory.
#
# Map task parallelism follows HDFS block sizing (default 128 MB).
# NUM_MAPPERS_HINT optionally sets dfs.blocksize instead so that
# the dataset is split into roughly that many map tasks.
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

INPUT=${1:?usage: run_hadoop.sh DATASET.TXT [OUTPUT_REPORT] [NUM_MAPPERS_HINT]}
OUT_LOCAL=${2:-/tmp/opencode/q8mr_hadoop_report.txt}
M_HINT=${3:-}
[ -f "$INPUT" ] || { echo "input not found: $INPUT"; exit 1; }
for bin in build/q8_mapper build/q8_combiner build/q8_reducer; do
    [ -x "$bin" ] || { echo "missing $bin — run make first"; exit 1; }
done

HADOOP=${HADOOP:-hadoop}
HDFS_DIR=/tmp/q8mr-$$
JOB_OUT=$HDFS_DIR-out
cleanup() { "$HADOOP" fs -rm -r -f "$HDFS_DIR" "$JOB_OUT" >/dev/null 2>&1 || true; }
trap cleanup EXIT

STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
[ -n "$STREAMING_JAR" ] || { echo "hadoop-streaming jar not found under \$HADOOP_HOME"; exit 1; }

HDFS_ARGS=(-put -f "$INPUT" "$HDFS_DIR/dataset.txt")
if [ -n "$M_HINT" ]; then
    # Aim for ~M map tasks by lowering the HDFS block size for this file.
    SIZE=$($HADOOP fs -stat -c "%s" "$INPUT" 2>/dev/null || stat -c %s "$INPUT")
    BS=$(( (SIZE + M_HINT - 1) / M_HINT ))
    [ "$BS" -lt 1048576 ] && BS=1048576
    HDFS_ARGS+=(-dfs.blocksize "$BS")
fi

# ---- 1. Upload the distributed input ----
"$HADOOP" fs -mkdir -p "$HDFS_DIR"
"$HADOOP" fs "${HDFS_ARGS[@]}"

# ---- 2+3+4. One Streaming job: map -> combine -> single reduce ----
"$HADOOP" jar "$STREAMING_JAR" \
    -D mapreduce.job.name=q8-weather-analytics \
    -D mapreduce.job.reduces=1 \
    -D mapreduce.map.memory.mb=2048 \
    -D mapreduce.reduce.memory.mb=2048 \
    -D stream.num.map.output.key.fields=1 \
    -files build/q8_mapper,build/q8_combiner,build/q8_reducer \
    -mapper ./q8_mapper \
    -combiner ./q8_combiner \
    -reducer ./q8_reducer \
    -input "$HDFS_DIR/dataset.txt" \
    -output "$JOB_OUT"

# ---- 5. Fetch the report ----
mkdir -p "$(dirname "$OUT_LOCAL")"
"$HADOOP" fs -cat "$JOB_OUT/part-*" > "$OUT_LOCAL"
echo "report written to $OUT_LOCAL"
