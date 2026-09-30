#!/bin/bash
# ============================================================
# run_hadoop.sh — Row-Row matrix multiplication on Hadoop
# Streaming (Hadoop 3.3.6 + YARN).
#
# Usage:
#   bash run_hadoop.sh A_FILE B_FILE OUTPUT_DIR
#
# Requirements:
#   * HADOOP_HOME set or `hadoop` on PATH (tested against 3.3.6)
#   * A_FILE: matrix A (m x n) — the distributed input. The sparse
#       format ("i<TAB>j<TAB>a_ij" per nonzero cell) is recommended;
#       a dense A is converted automatically (see DENSE note below).
#   * B_FILE: matrix B (n x p), dense or sparse.
#
# Pipeline (one MapReduce job):
#   1. A and B are uploaded to HDFS under /tmp/mrmatmul-$$/.
#   2. B is shipped to every mapper with -files — Hadoop stages it
#      into each task's working directory (the distributed cache),
#      which fulfills the "B fully replicated on every mapper node"
#      requirement. Each mapper then reads B once into memory.
#   3. mapper.py multiplies its rows of A with the local copy of B
#      and emits (row-i, "k:j:term"); Hadoop shuffles and groups all
#      pairs with the same row key on one reducer; combiner.py
#      pre-sums the terms per mapper to shrink the shuffle;
#      reducer.py sums each row's terms and reconstructs C.
#   4. C is copied back from the job output directory. Output format
#      is sparse ijv ("i<TAB>j<TAB>c_ij"); pipe through to_dense.py
#      for the dense presentation.
#
# DENSE note: a dense A file carries no row ids, and Hadoop Streaming
# does not tell a mapper which global rows of A it received (a custom
# InputFormat would). This script therefore converts dense A to sparse
# ijv locally before the upload — a pure I/O preparation step — so
# every A record keeps its global row index inside the record itself.
# ============================================================
set -euo pipefail
cd "$(dirname "$0")"

A_FILE=${1:?usage: run_hadoop.sh A_FILE B_FILE OUTPUT_DIR}
B_FILE=${2:?usage: run_hadoop.sh A_FILE B_FILE OUTPUT_DIR}
OUT_LOCAL=${3:-C_output.txt}
[ -f "$A_FILE" ] || { echo "A file not found: $A_FILE"; exit 1; }
[ -f "$B_FILE" ] || { echo "B file not found: $B_FILE"; exit 1; }

HADOOP=${HADOOP:-hadoop}
TMP=$(mktemp -d /tmp/mrmatmul.XXXXXX)
HDFS_DIR=/tmp/mrmatmul-$$
JOB_OUT=$HDFS_DIR-out
trap 'rm -rf "$TMP"; cleanup' EXIT

cleanup() { "$HADOOP" fs -rm -r -f "$HDFS_DIR" "$JOB_OUT" >/dev/null 2>&1 || true; }

STREAMING_JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar 2>/dev/null | head -1)
[ -n "$STREAMING_JAR" ] || { echo "hadoop-streaming jar not found under \$HADOOP_HOME"; exit 1; }

# Decide whether A needs conversion to sparse ijv (dense files have no row ids).
A_IS_DENSE=$(head -1 "$A_FILE" | grep -q $'\t' && echo 0 || echo 1)
if [ "$A_IS_DENSE" = "1" ]; then
    python3 to_sparse.py < "$A_FILE" > "$TMP/A_sparse.tsv"
    A_UPLOAD="$TMP/A_sparse.tsv"
else
    A_UPLOAD="$A_FILE"
fi

# Number of reducers = number of distinct C rows, capped (rows of C that
# exist in A; each row key maps to exactly one reducer).
NUM_REDUCERS=$(python3 - "$A_UPLOAD" <<'PY'
import sys
rows = set()
with open(sys.argv[1]) as stream:
    for line in stream:
        if line.strip():
            rows.add(int(line.split("\t", 1)[0]))
print(max(1, min(len(rows), 64)))
PY
)

# Ship B under a fixed name so the mapper command is independent of the
# user's local file name (-files stages files under their own basename).
cp "$B_FILE" "$TMP/B.txt"

# ---- 1. Upload the distributed input (A) and the replicated matrix (B) ----
"$HADOOP" fs -mkdir -p "$HDFS_DIR"
"$HADOOP" fs -put -f "$A_UPLOAD" "$HDFS_DIR/A.txt"
"$HADOOP" fs -put -f "$TMP/B.txt" "$HDFS_DIR/B.txt"

# ---- 2+3. One MapReduce job; B travels to every mapper via -files ----
"$HADOOP" jar "$STREAMING_JAR" \
    -D mapreduce.job.name=matrix-mult-row-row \
    -D mapreduce.job.reduces="$NUM_REDUCERS" \
    -D stream.num.map.output.key.fields=1 \
    -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator \
    -D mapreduce.partition.keycomparator.options="-k1,1n" \
    -files mapper.py,combiner.py,reducer.py,"$TMP/B.txt" \
    -mapper "python3 mapper.py B.txt" \
    -combiner "python3 combiner.py" \
    -reducer "python3 reducer.py" \
    -input "$HDFS_DIR/A.txt" \
    -output "$JOB_OUT"

# ---- 4. Collect C back from HDFS (sparse ijv; see to_dense.py) ----
"$HADOOP" fs -cat "$JOB_OUT/part-*" > "$OUT_LOCAL"
echo "wrote $OUT_LOCAL (sparse ijv format; use: python3 to_dense.py < $OUT_LOCAL)"
