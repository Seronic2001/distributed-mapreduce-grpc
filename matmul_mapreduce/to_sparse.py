#!/usr/bin/env python3
"""Convert a dense matrix file (one row per line) to sparse ijv format:

    i<TAB>j<TAB>value      (0-indexed, nonzero cells only, row-major)

Row/column ids come from the line/column positions. Zero rows produce no
lines, which is fine: the MapReduce pipeline and verify.py handle it.

Usage: to_sparse.py < dense.txt > sparse.txt
"""

import sys


def main():
    out = sys.stdout
    for i, raw in enumerate(sys.stdin):
        line = raw.rstrip("\n")
        if not line.strip():
            continue
        for j, value in enumerate(line.split()):
            if float(value) != 0.0:
                out.write("%d\t%d\t%g\n" % (i, j, float(value)))


if __name__ == "__main__":
    main()
