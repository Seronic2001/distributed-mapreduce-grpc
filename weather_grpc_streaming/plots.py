#!/usr/bin/env python3
"""
Generate gRPC streaming plots from benchmark CSV (Section 2 Q2).
Renders grpc_throughput.png and grpc_query_latency.png locally.
"""

import csv
import os
import statistics
import sys
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False


def main():
    if not HAS_MATPLOTLIB:
        print("Notice: matplotlib not installed; skipping plot generation.")
        return 0

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "results/grpc_benchmark.csv"
    if not os.path.exists(csv_path):
        print(f"plots.py: {csv_path} not found — skipping gRPC plots.")
        return 0

    out_dir = os.path.dirname(os.path.abspath(csv_path))
    os.makedirs(out_dir, exist_ok=True)

    with open(csv_path, newline="") as stream:
        reader = csv.DictReader(stream)
        rows = list(reader)

    if not rows:
        print("plots.py: No data rows found in", csv_path)
        return 0

    sizes = sorted(list({int(r["records"]) for r in rows}))
    target_size = sizes[0]
    workers = sorted(list({int(r["workers"]) for r in rows}))
    batches = sorted(list({int(r["batch_size"]) for r in rows}))

    # 1. Throughput plot
    medians = defaultdict(list)
    for row in rows:
        medians[(int(row["records"]), int(row["workers"]), int(row["batch_size"]))].append(
            float(row["recs_per_s"])
        )

    fig, ax = plt.subplots(figsize=(7, 4.5))
    for batch_size in batches:
        ys = [statistics.median(medians[(target_size, w, batch_size)]) for w in workers]
        ax.plot(workers, ys, marker="o", label=f"batch={batch_size}")

    ax.set_xlabel("workers")
    ax.set_ylabel("streaming throughput (records/s)")
    ax.set_title(f"gRPC streaming: throughput vs workers (N={target_size})")
    ax.legend()
    ax.grid(alpha=0.3)
    fig.tight_layout()
    throughput_png = os.path.join(out_dir, "grpc_throughput.png")
    fig.savefig(throughput_png, dpi=150)
    print("wrote", throughput_png)

    # 2. Query latency plot (if available)
    has_latency = any(row.get("query_latency_ms") for row in rows)
    if has_latency:
        qlats = defaultdict(list)
        for row in rows:
            if row.get("query_latency_ms"):
                qlats[(int(row["workers"]), int(row["batch_size"]))].append(
                    float(row["query_latency_ms"])
                )

        fig, ax = plt.subplots(figsize=(7, 4.5))
        for batch_size in batches:
            ys = [statistics.median(qlats.get((w, batch_size), [0])) for w in workers]
            ax.plot(workers, ys, marker="o", label=f"batch={batch_size}")

        ax.set_xlabel("workers")
        ax.set_ylabel("median GetAnalytics latency (ms)")
        ax.set_title("gRPC streaming: concurrent query latency")
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        query_png = os.path.join(out_dir, "grpc_query_latency.png")
        fig.savefig(query_png, dpi=150)
        print("wrote", query_png)


if __name__ == "__main__":
    main()
