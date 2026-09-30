#!/usr/bin/env python3
"""
Reduce phase of the Row-Row matrix multiplication.

The framework groups all intermediate pairs with the same key on one
reducer invocation, so this program receives, per key i, every partial
product produced anywhere in the cluster for row i of C:

    value = "k:j:term"   (or "i:j:partial_sum" when the combiner ran)

and reconstructs row i of C as

    C_i,j = sum over all terms with j fixed.

Usage:  reducer.py < shuffled_pairs
Input:  lines "i<TAB>k:j:term"  (not necessarily sorted; grouping is by key)
Output: the final matrix C in the same format as the inputs:
    dense  : one line per row, space separated  (rows ascending)
    sparse : "i<TAB>j<TAB>c_ij" per nonzero cell (row-major)

If the input contains a header line "C m n p" (emitted by run_local.sh /
run_hadoop.sh), the output is printed dense with exactly m rows; otherwise
the format matches the format of the first pair seen.
"""

import sys
from collections import defaultdict


def main():
    dims = None          # (m, n, p) when a header is present
    cells = defaultdict(float)   # (i, j) -> c_ij
    saw_sparse = False

    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        if dims is None and line.startswith("C "):
            _, m, n, p = line.split()
            dims = (int(m), int(n), int(p))
            continue
        key, _, value = line.partition("\t")
        i = int(key)
        parts = value.split(":")
        if len(parts) != 3:
            sys.stderr.write("reducer.py: bad pair value %r\n" % value)
            sys.exit(2)
        j = int(parts[1])
        cells[(i, j)] += float(parts[2])
        saw_sparse = True

    out = sys.stdout
    use_dense = dims is not None or not saw_sparse
    if use_dense:
        rows = dims[0] if dims else (max((i for i, _ in cells), default=-1) + 1)
        cols = dims[2] if dims else (max((j for _, j in cells), default=-1) + 1)
        width = max((len("%.10g" % v) for v in cells.values()), default=1)
        for i in range(rows):
            out.write(" ".join("%.10g" % cells.get((i, j), 0.0) for j in range(cols)))
            out.write("\n")
    else:
        for (i, j) in sorted(cells):
            if cells[(i, j)] != 0.0:
                out.write("%d\t%d\t%.10g\n" % (i, j, cells[(i, j)]))


if __name__ == "__main__":
    main()
