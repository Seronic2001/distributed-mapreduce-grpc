#!/usr/bin/env python3
"""Convert a sparse ijv matrix file back to dense form (one row per line).

Dimensions are inferred from the largest row/column id seen; pass --m/--p to
override (e.g. when the last rows/columns are all zero).

Usage: to_dense.py [--m M] [--p P] < sparse.txt > dense.txt
"""

import argparse
import sys


def main():
    ap = argparse.ArgumentParser(description="sparse ijv -> dense rows")
    ap.add_argument("--m", type=int, default=None, help="number of rows")
    ap.add_argument("--p", type=int, default=None, help="number of columns")
    args = ap.parse_args()

    cells = {}
    m = args.m if args.m is not None else 0
    p = args.p if args.p is not None else 0
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        i, j, value = line.split("\t")[:3]
        i, j, value = int(i), int(j), float(value)
        cells[(i, j)] = value
        m = max(m, i + 1)
        p = max(p, j + 1)

    out = sys.stdout
    for i in range(m):
        out.write(" ".join("%g" % cells.get((i, j), 0.0) for j in range(p)))
        out.write("\n")


if __name__ == "__main__":
    main()
