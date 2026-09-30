#!/usr/bin/env python3
"""
Map phase of the Row-Row matrix multiplication (C = A * B).

Row-Row method
--------------
Row i of C is the weighted sum of the rows of B, the weights being the
entries of row i of A:

    C_i = a_i,1 * B_1,:  +  a_i,2 * B_2,:  +  ...  +  a_i,n * B_n,:

Data distribution (as in classic MapReduce)
-------------------------------------------------
* Matrix A is the distributed input: the framework feeds this mapper only
  its assigned split of A on stdin, and every A row is processed
  independently (no inter-worker communication).
* Matrix B is fully replicated to every mapper node: under Hadoop
  Streaming it is shipped with `-files b.txt` (the distributed cache),
  and the mapper reads it from its local working directory before
  processing any A row. Locally, run_local.sh passes the same file.

Usage:
    mapper.py [--row-offset N] [B_FILE ...] < a_split.txt

--row-offset N gives the global index of the chunk's first row. Real Hadoop
inputs carry explicit row ids (sparse ijv format); for dense row files a
custom InputFormat would supply the record offset of the split the same way,
because a mapper that only sees "1 2" cannot know it holds row 7 of A.

Input formats (auto-detected from the first non-empty line):
  dense  : one row per line, space separated   ->  "a_i1 a_i2 ... a_in"
  sparse : "i<TAB>j<TAB>value" per nonzero cell (0-indexed)

Output (intermediate key/value pairs, TAB-separated):
    key   = "i"          (the row of C this partial product belongs to)
    value = "k:j:term"   with term = a_i,k * b_k,j

The shuffle groups all partial products of row i on one reducer, which
sums them into C_i. (A combiner may pre-sum them per mapper.)
"""

import sys


def fail(message):
    sys.stderr.write("mapper.py: %s\n" % message)
    sys.exit(2)


def load_b(paths):
    """Load the fully replicated matrix B into local memory.

    Returns (b_dense, b_sparse):
      b_dense  : dict  k -> [b_k0, b_k1, ...]
      b_sparse : dict (k, j) -> b_kj
    Exactly one of the two is populated, chosen per file by format.
    """
    b_dense = {}
    b_sparse = {}
    for path in paths:
        try:
            stream = open(path, "r")
        except OSError as error:
            fail("cannot open matrix B file %r: %s" % (path, error))
        with stream:
            first = True
            dense = False
            for raw in stream:
                line = raw.rstrip("\n")
                if not line.strip():
                    continue
                if first:
                    dense = "\t" not in line
                    first = False
                if dense:
                    if b_sparse:
                        fail("mixing dense and sparse B files is not supported")
                    row = [float(x) for x in line.split()]
                    b_dense[len(b_dense)] = row  # row k lands on line k
                else:
                    parts = line.split("\t")
                    if len(parts) != 3:
                        fail("bad sparse B line: %r" % line)
                    if b_dense:
                        fail("mixing dense and sparse B files is not supported")
                    k, j, value = int(parts[0]), int(parts[1]), float(parts[2])
                    if value != 0.0:
                        b_sparse[(k, j)] = value
    return b_dense, b_sparse


def map_a_rows(b_dense, b_sparse, row_offset):
    """Stream the mapper's assigned rows of A from stdin and emit pairs."""
    out = sys.stdout
    first = True
    dense = False
    local_row = 0
    for raw in sys.stdin:
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        if first:
            dense = "\t" not in line
            first = False
        if dense:
            values = [float(x) for x in line.split()]
            i = row_offset + local_row
            local_row += 1
            terms = ((k, a_ik) for k, a_ik in enumerate(values) if a_ik != 0.0)
        else:
            parts = line.split("\t")
            if len(parts) != 3:
                fail("bad sparse A line: %r" % line)
            i, k, a_ik = int(parts[0]), int(parts[1]), float(parts[2])
            if a_ik == 0.0:
                continue
            terms = ((k, a_ik),)
        emit_row_terms(out, i, terms, b_dense, b_sparse)


def emit_row_terms(out, i, terms, b_dense, b_sparse):
    """Emit key=i, value=i:j:c_ij for row i, computed as the weighted sum: C_i = sum_k a_ik * B_k,:"""
    row_c = {}
    for k, a_ik in terms:
        if b_dense:
            row_b = b_dense.get(k)
            if row_b is None:
                fail("row %d of A references B row %d, but B has only %d rows"
                     % (i, k, len(b_dense)))
            for j, b_kj in enumerate(row_b):
                if b_kj != 0.0:
                    row_c[j] = row_c.get(j, 0.0) + a_ik * b_kj
        else:
            for (bk, bj), b_kj in b_sparse.items():
                if bk == k:
                    row_c[bj] = row_c.get(bj, 0.0) + a_ik * b_kj
    for j, val in row_c.items():
        if val != 0.0:
            out.write("%d\t%d:%d:%.10g\n" % (i, i, j, val))


def main():
    row_offset = 0
    b_paths = []
    for arg in sys.argv[1:]:
        if arg.startswith("--row-offset="):
            row_offset = int(arg.partition("=")[2])
        elif arg == "--row-offset":
            fail("--row-offset requires a value, e.g. --row-offset=7")
        else:
            b_paths.append(arg)
    if not b_paths:
        fail("usage: mapper.py [--row-offset N] B_FILE [B_FILE ...] < a_split.txt")
    b_dense, b_sparse = load_b(b_paths)
    if not b_dense and not b_sparse:
        fail("matrix B is empty")
    map_a_rows(b_dense, b_sparse, row_offset)


if __name__ == "__main__":
    main()
