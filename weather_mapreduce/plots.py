#!/usr/bin/env python3
"""
Render plots for the MPI vs MapReduce comparison (Section 2 Q1).

Reads results/comparison.csv (written by benchmark.sh) and writes:

  results/total_time.png    total time vs workers, one curve per engine/size
  results/speedup.png       speedup over sequential vs workers

Missing engines (e.g. MPI rows skipped because mpicxx was unavailable)
are simply absent from the plots. Rerun on the cluster where the full
baseline toolchain exists to fill them in.

Usage: python3 plots.py
"""

import csv
import os
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
        print("Notice: matplotlib not installed; skipping plot generation (plots will be generated locally).")
        return 0

    default_csv = os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "comparison.csv")
    csv_path = sys.argv[1] if len(sys.argv) > 1 else ("results/comparison.csv" if os.path.exists("results/comparison.csv") else default_csv)
    if not os.path.exists(csv_path):
        sys.exit(f"plots.py: {csv_path} not found — run benchmark.sh first")

    rows = list(csv.DictReader(open(csv_path)))
    series = defaultdict(list)   # (engine, workers) -> {records: total}
    seq = {}                     # records -> sequential time
    for row in rows:
        total = float(row["total_s"]) if row["total_s"] else None
        if total is None:
            continue
        engine, workers, records = row["engine"], int(row["workers"]), int(row["records"])
        if engine == "sequential":
            seq[records] = total
        else:
            series[(engine, workers)].append((records, total))

    sizes = sorted({int(r["records"]) for r in rows})

    # Color palette per N:
    color_map = {
        (10000, "mapreduce"): "#1f77b4",
        (10000, "mpi"): "#2ca02c",
        (10000, "seq"): "#17becf",
        (100000, "mapreduce"): "#ff7f0e",
        (100000, "mpi"): "#d62728",
        (100000, "seq"): "#e377c2",
        (1000000, "mapreduce"): "#9467bd",
        (1000000, "mpi"): "#8c564b",
        (1000000, "seq"): "#7f7f7f",
    }

    # ---- total time vs workers (log scale) ----
    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    for records in sizes:
        for engine in ["mapreduce", "mpi"]:
            xs, ys = [], []
            for (e, w), points in series.items():
                if e != engine:
                    continue
                point = dict(points).get(records)
                if point is not None:
                    xs.append(w)
                    ys.append(point)
            if xs:
                xs_ys = sorted(zip(xs, ys))
                xs, ys = [p[0] for p in xs_ys], [p[1] for p in xs_ys]
                marker = "o" if engine == "mapreduce" else "s"
                linestyle = "-" if engine == "mapreduce" else "--"
                c = color_map.get((records, engine))
                ax.plot(xs, ys, marker=marker, linestyle=linestyle, linewidth=1.5,
                        color=c, label=f"{engine} N={records}")

    for records in sizes:
        if records in seq:
            c = color_map.get((records, "seq"), "gray")
            ax.axhline(seq[records], linestyle=":", linewidth=1.4, color=c, alpha=0.9,
                       label=f"seq N={records} ({seq[records]*1000:.1f}ms)")

    ax.set_yscale("log")
    ax.set_ylim(0.001, 10.0)
    ax.set_xlabel("workers (MPI ranks / map tasks)")
    ax.set_ylabel("total time (s) [log scale]")
    ax.set_title("Q8 weather: MPI vs MapReduce — total time (log scale)")
    ax.set_xticks([1, 2, 4, 8, 16])
    ax.legend(fontsize=7.2, ncol=3, loc="upper center", framealpha=0.9)
    ax.grid(True, which="both", linestyle="--", alpha=0.3)
    fig.tight_layout()
    out_dir = os.path.dirname(os.path.abspath(csv_path))
    os.makedirs(out_dir, exist_ok=True)
    total_time_png = os.path.join(out_dir, "total_time.png")
    speedup_png = os.path.join(out_dir, "speedup.png")

    fig.savefig(total_time_png, dpi=150)

    # ---- speedup over sequential ----
    fig, ax = plt.subplots(figsize=(7.2, 4.8))
    for records in sizes:
        base = seq.get(records)
        if not base:
            continue
        engines = sorted({e for (e, _) in series})
        for engine in engines:
            xs, ys = [], []
            for (e, w), points in series.items():
                if e != engine:
                    continue
                point = dict(points).get(records)
                if point is not None:
                    xs.append(w)
                    ys.append(base / point)
            if xs:
                xs_ys = sorted(zip(xs, ys))
                xs, ys = [p[0] for p in xs_ys], [p[1] for p in xs_ys]
                marker = "o" if engine == "mapreduce" else "s"
                linestyle = "-" if engine == "mapreduce" else "--"
                c = color_map.get((records, engine))
                ax.plot(xs, ys, marker=marker, linestyle=linestyle, linewidth=1.5,
                        color=c, label=f"{engine} N={records}")
    ax.plot([1, max(w for (_, w) in series) or 8], [1, 1],
            color="gray", linestyle=":", linewidth=1, label="sequential baseline (1.0x)")
    ax.set_xlabel("workers (MPI ranks / map tasks)")
    ax.set_ylabel("speedup vs sequential")
    ax.set_title("Q8 weather: speedup")
    ax.set_xticks([1, 2, 4, 8, 16])
    ax.legend(fontsize=7.2, ncol=2, loc="upper right")
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(speedup_png, dpi=150)

    # Sync to report/figures if present
    repo_root = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
    report_figures = os.path.join(repo_root, "report", "figures")
    if os.path.isdir(report_figures):
        import shutil
        shutil.copy2(total_time_png, os.path.join(report_figures, "total_time.png"))
        shutil.copy2(speedup_png, os.path.join(report_figures, "speedup.png"))
        print(f"synced plots to {report_figures}")

    print(f"wrote {total_time_png} and {speedup_png}")


if __name__ == "__main__":
    main()
