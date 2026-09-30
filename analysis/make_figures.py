#!/usr/bin/env python3
"""
Regenerate every benchmark figure from the committed result CSVs.

Inputs (relative to the repository root):
    matmul_mapreduce/results/matmul_timings.csv
    weather_mapreduce/results/comparison.csv
    weather_grpc_streaming/results/grpc_sweep.csv

Outputs (analysis/figures/):
    matmul_stages.png      S1 Q1: stage times of the latest R=8 and R=16 runs
    mr_total_time.png      S2 Q1: total time vs workers, one panel per input size
    mr_stages_1m.png       S2 Q1: MapReduce stage breakdown at N = 10^6
    grpc_throughput.png    S2 Q2: throughput vs workers, one panel per input size

Usage:  python3 analysis/make_figures.py      (needs matplotlib)
"""

import csv
import os
import statistics
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

REPORT = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(REPORT)
FIGURES = os.path.join(REPORT, "figures")

# Categorical slots 1-4 of the validated reference palette (light mode),
# assigned in fixed order. Text and axes stay in neutral ink.
SERIES = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100"]
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
GRID = "#e4e3df"
REFERENCE = "#8a8984"
MARKERS = ["o", "s", "^", "D"]

plt.rcParams.update({
    "font.size": 9,
    "axes.edgecolor": INK_SECONDARY,
    "axes.labelcolor": INK,
    "axes.titlesize": 9.5,
    "axes.titleweight": "bold",
    "xtick.color": INK_SECONDARY,
    "ytick.color": INK_SECONDARY,
    "legend.frameon": False,
    "legend.fontsize": 8,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "savefig.bbox": "tight",
    "savefig.dpi": 200,
})


def style_axes(ax, grid_axis="y"):
    ax.grid(axis=grid_axis, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)


def read_csv(relative_path):
    with open(os.path.join(REPO, relative_path), newline="") as handle:
        return list(csv.DictReader(handle))


# ------------------------------------------------------------------ S1 Q1 --
def matmul_stages():
    rows = [r for r in read_csv("matmul_mapreduce/results/matmul_timings.csv")
            if r["input"] == "A_400.txt"]
    # The CSV is append-only: plot the most recent R=8 and R=16 run.
    def latest(mappers):
        return [r for r in rows if r["num_mappers"] == str(mappers)][-1]

    stages = [("map_s", "map"), ("shuffle1_s", "shuffle"),
              ("combine_s", "combine"), ("reduce_s", "reduce")]
    labels = ["R = 8", "R = 16"]
    runs = [latest(8), latest(16)]

    fig, ax = plt.subplots(figsize=(5.4, 2.0))
    left = [0.0, 0.0]
    for index, (column, name) in enumerate(stages):
        widths = [float(run[column]) for run in runs]
        ax.barh(labels, widths, left=left, height=0.55, color=SERIES[index],
                edgecolor="white", linewidth=1.5, label=name)
        left = [a + b for a, b in zip(left, widths)]
    for y, run in enumerate(runs):
        total = float(run["total_s"])
        ax.text(total, y, "  %.2f s" % total, va="center", color=INK, fontsize=8)
    ax.set_xlim(0, max(float(r["total_s"]) for r in runs) * 1.2)
    ax.invert_yaxis()
    ax.set_title("Matrix multiplication 400 x 400: time per stage", loc="left")
    ax.set_xlabel("seconds")
    style_axes(ax, grid_axis="x")
    ax.legend(ncol=4, loc="upper left", bbox_to_anchor=(0, -0.35))
    fig.savefig(os.path.join(FIGURES, "matmul_stages.png"))
    plt.close(fig)


# ------------------------------------------------------------------ S2 Q1 --
def mr_figures():
    rows = read_csv("weather_mapreduce/results/comparison.csv")
    sizes = sorted({int(r["records"]) for r in rows})

    fig, axes = plt.subplots(1, len(sizes), figsize=(7.2, 2.5), sharey=True)
    for ax, size in zip(axes, sizes):
        subset = [r for r in rows if int(r["records"]) == size]
        sequential = next(float(r["total_s"]) for r in subset
                          if r["engine"] == "sequential")
        ax.axhline(sequential, color=REFERENCE, linestyle="--", linewidth=1.2,
                   label="sequential")
        for index, (engine, label) in enumerate([("mpi", "MPI"),
                                                 ("mapreduce", "MapReduce")]):
            points = sorted((int(r["workers"]), float(r["total_s"]))
                            for r in subset if r["engine"] == engine)
            ax.plot([p[0] for p in points], [p[1] for p in points],
                    color=SERIES[index], marker=MARKERS[index], markersize=5,
                    linewidth=1.6, label=label)
        ax.set_xscale("log", base=2)
        ax.set_xticks([1, 2, 4, 8, 16])
        ax.set_xticklabels(["1", "2", "4", "8", "16"])
        ax.set_yscale("log")
        ax.set_title("N = {:,}".format(size), loc="left")
        ax.set_xlabel("processes P / map tasks M")
        style_axes(ax)
    axes[0].set_ylabel("total time (s, log scale)")
    axes[-1].legend(loc="lower right")
    fig.savefig(os.path.join(FIGURES, "mr_total_time.png"))
    plt.close(fig)

    # Stage breakdown at the largest size; "other" is the part of the total
    # not covered by the three timed stages (splitting, staging, launch).
    largest = [r for r in rows
               if int(r["records"]) == sizes[-1] and r["engine"] == "mapreduce"]
    largest.sort(key=lambda r: int(r["workers"]))
    labels = ["M = %s" % r["workers"] for r in largest]
    parts = [("map_s", "map"), ("combine_s", "combine"), ("reduce_s", "reduce")]
    fig, ax = plt.subplots(figsize=(4.6, 2.4))
    bottom = [0.0] * len(largest)
    for index, (column, name) in enumerate(parts):
        heights = [float(r[column]) for r in largest]
        ax.bar(labels, heights, bottom=bottom, width=0.6, color=SERIES[index],
               edgecolor="white", linewidth=1.5, label=name)
        bottom = [a + b for a, b in zip(bottom, heights)]
    other = [max(0.0, float(r["total_s"]) - b) for r, b in zip(largest, bottom)]
    if max(other) > 0.02:   # only draw (and list) it when it is visible
        ax.bar(labels, other, bottom=bottom, width=0.6, color=SERIES[3],
               edgecolor="white", linewidth=1.5,
               label="other (split, staging, launch)")
    for x, r in enumerate(largest):
        ax.text(x, float(r["total_s"]), "%.2f" % float(r["total_s"]),
                ha="center", va="bottom", fontsize=8, color=INK)
    ax.set_ylabel("seconds")
    ax.set_title("MapReduce stages, N = {:,}".format(sizes[-1]), loc="left")
    ax.set_ylim(0, max(float(r["total_s"]) for r in largest) * 1.12)
    style_axes(ax)
    ax.legend(loc="upper right")
    fig.savefig(os.path.join(FIGURES, "mr_stages_1m.png"))
    plt.close(fig)


# ------------------------------------------------------------------ S2 Q2 --
def grpc_throughput():
    rows = read_csv("weather_grpc_streaming/results/grpc_sweep.csv")
    samples = defaultdict(list)
    for r in rows:
        key = (int(r["records"]), int(r["workers"]), int(r["batch_size"]))
        samples[key].append(float(r["recs_per_s"]) / 1000.0)
    sizes = sorted({k[0] for k in samples})
    batches = sorted({k[2] for k in samples})

    fig, axes = plt.subplots(1, len(sizes), figsize=(7.2, 2.6), sharey=True)
    for ax, size in zip(axes, sizes):
        workers = sorted({k[1] for k in samples if k[0] == size})
        for index, batch in enumerate(batches):
            medians, low, high = [], [], []
            for w in workers:
                values = samples[(size, w, batch)]
                m = statistics.median(values)
                medians.append(m)
                low.append(m - min(values))
                high.append(max(values) - m)
            offset = [w * (1 + 0.06 * (index - 1)) for w in workers]
            ax.errorbar(offset, medians, yerr=[low, high], color=SERIES[index],
                        marker=MARKERS[index], markersize=5, linewidth=1.6,
                        elinewidth=1.0, capsize=2.5,
                        label="batch = {:,}".format(batch))
        ax.set_xscale("log", base=2)
        ax.set_xticks(workers)
        ax.set_xticklabels([str(w) for w in workers])
        ax.set_title("N = {:,}".format(size), loc="left")
        ax.set_xlabel("workers W")
        style_axes(ax)
    axes[0].set_ylabel("throughput (k records/s)")
    axes[0].legend(ncol=3, loc="upper left", bbox_to_anchor=(0, -0.25))
    fig.savefig(os.path.join(FIGURES, "grpc_throughput.png"))
    plt.close(fig)


def main():
    os.makedirs(FIGURES, exist_ok=True)
    matmul_stages()
    mr_figures()
    grpc_throughput()
    print("figures written to", FIGURES)


if __name__ == "__main__":
    main()
