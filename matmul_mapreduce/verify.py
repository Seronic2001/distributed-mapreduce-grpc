#!/usr/bin/env python3
"""
Brute-force reference implementation of C = A * B (the verification oracle).

Reads A and B in either format (dense or sparse), computes the product with
plain triple-loop arithmetic, and prints C in the same dense/sparse format
the reducer emits, so its output can be diffed byte-for-byte against the
MapReduce pipeline output.

Usage:
    verify.py A_FILE B_FILE            # prints C in the format of A_FILE
    verify.py A_FILE B_FILE --sparse   # force sparse output
"""

import argparse
import sys


def load(path):
    """Load a matrix file -> (rows, cols, dict {(i, j): value}, dense?)."""
    rows = cols = 0
    cells = {}
    with open(path, "r") as stream:
        first = True
        dense = False
        row_index = 0
        for raw in stream:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            if first:
                dense = "\t" not in line
                first = False
            if dense:
                values = [float(x) for x in line.split()]
                if cols == 0:
                    cols = len(values)
                elif len(values) != cols:
                    sys.exit("verify.py: ragged dense matrix in %r" % path)
                for j, value in enumerate(values):
                    if value != 0.0:
                        cells[(row_index, j)] = value
                row_index += 1
            else:
                parts = line.split("\t")
                if len(parts) != 3:
                    sys.exit("verify.py: bad sparse line in %r: %r" % (path, line))
                i, j, value = int(parts[0]), int(parts[1]), float(parts[2])
                if value != 0.0:
                    cells[(i, j)] = value
                rows = max(rows, i + 1)
                cols = max(cols, j + 1)
    if dense:
        rows = row_index
    return rows, cols, cells, dense


def main():
    parser = argparse.ArgumentParser(description="Brute-force C = A * B oracle")
    parser.add_argument("a_file")
    parser.add_argument("b_file")
    parser.add_argument("--sparse", action="store_true",
                        help="print C in sparse i<TAB>j<TAB>v format")
    args = parser.parse_args()

    m, n, a, _ = load(args.a_file)
    n2, p, b, _ = load(args.b_file)
    if n != n2:
        sys.exit("verify.py: dimension mismatch: A is %dx%d, B is %dx%d"
                 % (m, n, n2, p))

    # Plain triple loop, integer-exact for integer inputs.
    c = {}
    for (i, k), a_ik in a.items():
        for (k2, j), b_kj in b.items():
            if k == k2:
                c[(i, j)] = c.get((i, j), 0.0) + a_ik * b_kj

    out = sys.stdout
    if args.sparse:
        for (i, j) in sorted(c):
            if c[(i, j)] != 0.0:
                out.write("%d\t%d\t%.10g\n" % (i, j, c[(i, j)]))
    else:
        for i in range(m):
            out.write(" ".join("%.10g" % c.get((i, j), 0.0) for j in range(p)))
            out.write("\n")


if __name__ == "__main__":
    main()
