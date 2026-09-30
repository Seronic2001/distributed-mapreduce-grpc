#!/usr/bin/env python3
"""
Combiner (optional local pre-aggregation) for the Row-Row matrix multiplication.

Runs on each mapper node after the map phase, before the shuffle. It groups
the mapper's own intermediate pairs by key and pre-sums the terms of each
(i, j) cell, so the shuffle carries far less data.

Hadoop treats the combiner as a "mini-reducer", so the same program is also
used as the job's combiner: its output (key<TAB>i:j:sum) is valid mapper
output, and the reducer accepts pre-summed terms unchanged.

Usage:  combiner.py  < mapper_output
Input:  "i<TAB>k:j:term"   (terms may already be partial sums)
Output: "i<TAB>i:j:sum"    (one line per nonzero cell, i ascending)
"""

import sys
from collections import defaultdict


def main():
    cells = defaultdict(float)   # (i, j) -> partial sum
    order = {}                   # (i, j) -> first-seen sequence (for stable output)
    for raw in sys.stdin:
        line = raw.strip()
        if not line:
            continue
        key, _, value = line.partition("\t")
        i = int(key)
        k_s, j_s, term_s = value.split(":")
        j = int(j_s)
        if (i, j) not in cells:
            order[(i, j)] = len(order)
        cells[(i, j)] += float(term_s)
    for (i, j) in sorted(order, key=order.get):
        # Reducer accepts "k:j:sum"; combiner output must remain valid
        # mapper-style pairs, so we keep the k field (value of a_ik is not
        # needed any more once the multiplication has happened).
        sys.stdout.write("%d\t%d:%d:%.10g\n" % (i, i, j, cells[(i, j)]))


if __name__ == "__main__":
    main()
