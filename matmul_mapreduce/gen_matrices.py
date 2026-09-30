#!/usr/bin/env python3
"""
Generate reproducible test matrices A (m x n) and B (n x p).

Entries are integers in [-5, 5] (a density option zeroes most of them out),
so products stay exact and both the MapReduce pipeline and the oracle can be
compared byte-for-byte.

Usage:
    gen_matrices.py --m 3 --n 2 --p 3 --seed 1 [--density 1.0] \
        [--sparse] [--out-a a.txt] [--out-b b.txt]

With --sparse the files are emitted in the "i<TAB>j<TAB>v" format (only
nonzero cells, and fully empty rows simply have no lines), otherwise one
dense row per line. Note that a sparse file can be empty when the matrix is
all zeros, which the mapper/reducer handle correctly.
"""

import argparse
import random


def emit(path, rows, cols, cells, sparse):
    if sparse:
        lines = ["%d\t%d\t%d" % (i, j, cells[(i, j)])
                 for (i, j) in sorted(cells) if cells[(i, j)] != 0]
    else:
        lines = [" ".join(str(cells.get((i, j), 0)) for j in range(cols))
                 for i in range(rows)]
    text = "\n".join(lines) + "\n" if lines else ""
    if path:
        with open(path, "w") as stream:
            stream.write(text)
    else:
        print(text, end="")


def main():
    ap = argparse.ArgumentParser(description="Generate test matrices A and B")
    ap.add_argument("--m", type=int, required=True, help="rows of A / C")
    ap.add_argument("--n", type=int, required=True, help="cols of A, rows of B")
    ap.add_argument("--p", type=int, required=True, help="cols of B / C")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--density", type=float, default=1.0,
                    help="probability a cell is nonzero (0 < d <= 1)")
    ap.add_argument("--sparse", action="store_true", help="emit ijv format")
    ap.add_argument("--out-a", default=None)
    ap.add_argument("--out-b", default=None)
    args = ap.parse_args()

    rng = random.Random(args.seed)
    density = min(1.0, max(0.001, args.density))

    a = {}
    for i in range(args.m):
        for k in range(args.n):
            if rng.random() < density:
                a[(i, k)] = rng.randint(-5, 5)
    b = {}
    for k in range(args.n):
        for j in range(args.p):
            if rng.random() < density:
                b[(k, j)] = rng.randint(-5, 5)

    emit(args.out_a, args.m, args.n, a, args.sparse)
    emit(args.out_b, args.n, args.p, b, args.sparse)


if __name__ == "__main__":
    main()
