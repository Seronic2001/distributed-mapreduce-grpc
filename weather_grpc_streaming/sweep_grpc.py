#!/usr/bin/env python3
"""
Reproducible grid sweep for the gRPC weather streaming system (Section 2 Q2).

Runs the complete configuration grid

    sizes × workers × batch sizes × repetitions

and records, per configuration: end-to-end time, streaming throughput
(records/s) and — with --queries — the median GetAnalytics latency measured
by a concurrent probe client during the last repetition.

Outputs (in results/):
    grpc_sweep.csv          one row per repetition (append/resume aware)
    grpc_sweep_summary.md   per-size tables of medians + speedup vs W=1
    grpc_sweep_throughput.png   throughput vs workers, one panel per size
    grpc_sweep_latency.png      query latency vs workers (with --queries)
    grpc_sweep_manifest.json    full reproducibility manifest

Resume: already-measured (size, workers, batch, rep) rows in the CSV are
skipped, so an interrupted sweep can be rerun and continues where it left
off. Delete the CSV to start fresh.

Defaults (moderate, laptop-friendly; expect a few minutes):

    --sizes 100000 1000000 --workers 1 2 4 8 --batches 1000 4000 16000 --reps 3

Example, heavier:

    python3 sweep_grpc.py --sizes 10000 100000 1000000 \
        --workers 1 2 4 8 --batches 250 1000 4000 16000 --reps 3 --queries 1
"""

import argparse
import csv
import json
import os
import platform
import signal
import statistics
import subprocess
import sys
import time
from collections import defaultdict

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = HERE
REPO = os.path.dirname(ROOT)
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

import benchmark_grpc as bg                     # noqa: E402  (reuse lifecycle)
import weather_pb2                              # noqa: E402
import weather_pb2_grpc                         # noqa: E402

RESULTS = os.path.join(ROOT, "results")
CSV_PATH = os.path.join(RESULTS, "grpc_sweep.csv")
MANIFEST_PATH = os.path.join(RESULTS, "grpc_sweep_manifest.json")


def load_done_keys():
    """Resume support: (size, workers, batch, rep) already in the CSV."""
    done = set()
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH) as stream:
            for row in csv.DictReader(stream):
                try:
                    done.add((int(row["records"]), int(row["workers"]),
                              int(row["batch_size"]), int(row["rep"])))
                except (KeyError, ValueError):
                    continue
    return done


def write_header_if_needed():
    os.makedirs(RESULTS, exist_ok=True)
    if not os.path.exists(CSV_PATH) or os.path.getsize(CSV_PATH) == 0:
        with open(CSV_PATH, "w", newline="") as stream:
            csv.writer(stream).writerow(
                ["records", "workers", "batch_size", "rep", "elapsed_s",
                 "recs_per_s", "query_latency_ms"])


def append_rows(rows):
    with open(CSV_PATH, "a", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=[
            "records", "workers", "batch_size", "rep", "elapsed_s",
            "recs_per_s", "query_latency_ms"])
        writer.writerows(rows)


def load_all_rows():
    with open(CSV_PATH) as stream:
        return list(csv.DictReader(stream))


def render_summary(rows, args):
    """Markdown tables: medians per (workers, batch) + speedup vs W=1."""
    med = defaultdict(list)
    qmed = defaultdict(list)
    emed = defaultdict(list)
    for row in rows:
        med[(int(row["records"]), int(row["workers"]),
             int(row["batch_size"]))].append(float(row["recs_per_s"]))
        emed[(int(row["records"]), int(row["workers"]),
              int(row["batch_size"]))].append(float(row["elapsed_s"]))
        if row["query_latency_ms"]:
            qmed[(int(row["records"]), int(row["workers"]),
                  int(row["batch_size"]))].append(
                float(row["query_latency_ms"]))

    lines = ["# gRPC weather streaming — sweep summary", ""]
    lines.append("Throughput = records/s, median of %d rep(s). "
                 "Speedup is vs W=1 for the same batch size. "
                 "Query latency = median GetAnalytics latency measured by a "
                 "concurrent probe during ingestion (only when --queries)."
                 % args.reps)
    lines.append("")
    for size in sorted({k[0] for k in med}):
        lines.append("## N = %s records" % f"{size:,}")
        lines.append("")
        lines.append("| workers | batch | throughput rec/s | elapsed s | speedup vs W=1 | query ms |")
        lines.append("|---|---|---|---|---|---|")
        base = {}
        for batch in sorted({k[2] for k in med if k[0] == size}):
            values = med.get((size, 1, batch))
            if values:
                base[batch] = statistics.median(values)
        for workers in sorted({k[1] for k in med if k[0] == size}):
            for batch in sorted({k[2] for k in med if k[0] == size}):
                values = med.get((size, workers, batch))
                if not values:
                    continue
                throughput = statistics.median(values)
                elapsed = statistics.median(emed[(size, workers, batch)])
                speedup = (throughput / base[batch]
                           if batch in base and base[batch] else 1.0)
                queries = qmed.get((size, workers, batch))
                query_text = ("%.2f" % statistics.median(queries)
                              if queries else "—")
                lines.append("| %d | %d | %.0f | %.3f | %.2fx | %s |"
                             % (workers, batch, throughput, elapsed, speedup,
                                query_text))
        lines.append("")
    best = {}
    for (size, workers, batch), values in med.items():
        value = statistics.median(values)
        if size not in best or value > best[size][1]:
            best[size] = ((workers, batch), value)
    lines.append("## Best configuration per size")
    lines.append("")
    for size, ((workers, batch), value) in sorted(best.items()):
        lines.append("- N=%s: **W=%d, batch=%d** → %.0f rec/s"
                     % (f"{size:,}", workers, batch, value))
    lines.append("")
    return "\n".join(lines)


def render_plots(rows, args):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        print("matplotlib unavailable — skipping plots")
        return

    med = defaultdict(list)
    qmed = defaultdict(list)
    for row in rows:
        med[(int(row["records"]), int(row["workers"]),
             int(row["batch_size"]))].append(float(row["recs_per_s"]))
        if row["query_latency_ms"]:
            qmed[(int(row["records"]), int(row["workers"]),
                  int(row["batch_size"]))].append(
                float(row["query_latency_ms"]))

    sizes = sorted({k[0] for k in med})
    batches = sorted({k[2] for k in med})
    workers = sorted({k[1] for k in med})

    # ---- throughput panels (one per size) ----
    fig, axes = plt.subplots(1, len(sizes), figsize=(6 * len(sizes), 4.5),
                             squeeze=False)
    for column, size in enumerate(sizes):
        ax = axes[0][column]
        for batch in batches:
            ys = [statistics.median(med[(size, w, batch)]) if med.get((size, w, batch)) else None
                  for w in workers]
            xs = [w for w, y in zip(workers, ys) if y is not None]
            clean = [y for y in ys if y is not None]
            if xs:
                ax.plot(xs, clean, marker="o", label="batch=%d" % batch)
        ax.set_xlabel("workers")
        ax.set_ylabel("throughput (records/s)")
        ax.set_title("N = %s" % f"{size:,}")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
    fig.suptitle("gRPC streaming throughput vs workers")
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS, "grpc_sweep_throughput.png"), dpi=150)

    # ---- query latency panel ----
    if args.queries and qmed:
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for batch in batches:
            xs, ys = [], []
            for w in workers:
                values = qmed.get((sizes[-1], w, batch))
                if values:
                    xs.append(w)
                    ys.append(statistics.median(values))
            if xs:
                ax.plot(xs, ys, marker="o", label="batch=%d" % batch)
        ax.set_xlabel("workers")
        ax.set_ylabel("median GetAnalytics latency (ms)")
        ax.set_title("Concurrent query latency during ingestion (N=%s)"
                     % f"{sizes[-1]:,}")
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS, "grpc_sweep_latency.png"), dpi=150)
    print("wrote results/grpc_sweep_throughput.png"
          + (" and results/grpc_sweep_latency.png" if args.queries and qmed else ""))


def write_manifest(args, started, ended, total_rows):
    import grpc as grpc_module
    manifest = {
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(started)),
        "ended_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ended)),
        "grid": {"sizes": args.sizes, "workers": args.workers,
                 "batch_sizes": args.batches, "reps": args.reps,
                 "queries": bool(args.queries),
                 "stations": args.stations, "seed": args.seed},
        "environment": {"python": platform.python_version(),
                        "platform": platform.platform(),
                        "cpu_count": os.cpu_count(),
                        "grpc_version": getattr(grpc_module, "__version__",
                                                "unknown")},
        "repetitions_recorded": total_rows,
        "datasets": {},
        "notes": ["replay at max speed (no artificial rate limit)",
                  "throughput counted from dataset header N over end-to-end "
                  "client wall time; server startup is excluded (system is "
                  "started before timing begins)",
                  "system processes are SIGINT-stopped between configs"],
    }
    for size in args.sizes:
        path = bg.ensure_dataset(size, stations=args.stations, seed=args.seed)
        import hashlib
        digest = hashlib.sha256()
        with open(path, "rb") as stream:
            for chunk in iter(lambda: stream.read(1 << 20), b""):
                digest.update(chunk)
        manifest["datasets"][str(size)] = {
            "path": path, "sha256": digest.hexdigest(),
            "bytes": os.path.getsize(path)}
    with open(MANIFEST_PATH, "w") as stream:
        json.dump(manifest, stream, indent=2)
    print("wrote", MANIFEST_PATH)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sizes", type=int, nargs="+",
                        default=[100000, 1000000])
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4, 8])
    parser.add_argument("--batches", type=int, nargs="+",
                        default=[1000, 4000, 16000])
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--queries", action="store_true",
                        help="probe GetAnalytics concurrently (last rep only)")
    parser.add_argument("--stations", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--base-port", type=int, default=51000)
    args = parser.parse_args()

    write_header_if_needed()
    done = load_done_keys()
    planned = len(args.sizes) * len(args.workers) * len(args.batches) * args.reps
    print("grid: %d sizes x %d worker-counts x %d batch-sizes x %d reps = %d runs"
          % (len(args.sizes), len(args.workers), len(args.batches), args.reps,
             planned))
    if done:
        print("resuming: %d runs already recorded in %s" % (len(done), CSV_PATH))

    started = time.time()
    port = args.base_port
    collected = []
    interrupted = False
    try:
        for size in args.sizes:
            dataset = bg.ensure_dataset(size, stations=args.stations,
                                        seed=args.seed)
            for workers in args.workers:
                for batch in args.batches:
                    for rep in range(1, args.reps + 1):
                        key = (size, workers, batch, rep)
                        if key in done:
                            continue
                        probe = args.queries and rep == args.reps
                        procs, coord_addr, _stub, channel = \
                            bg.start_system(port, workers)
                        try:
                            elapsed, records, latencies = bg.run_once(
                                coord_addr, dataset, batch,
                                query_probes=probe)
                        finally:
                            bg.stop_system(procs)
                            channel.close()
                            time.sleep(0.3)
                        row = {
                            "records": size, "workers": workers,
                            "batch_size": batch, "rep": rep,
                            "elapsed_s": "%.3f" % elapsed,
                            "recs_per_s": "%.0f" % (records / elapsed
                                                    if elapsed else 0),
                            "query_latency_ms": (
                                "%.2f" % (statistics.median(latencies) * 1000)
                                if latencies else ""),
                        }
                        append_rows([row])
                        collected.append(row)
                        done.add(key)
                        print("N=%-8d W=%d B=%-5d rep%d: %6.2fs  %8.0f rec/s"
                              "  query %s"
                              % (size, workers, batch, rep, elapsed,
                                 records / elapsed if elapsed else 0,
                                 ("%.2f ms" % (statistics.median(latencies) * 1000)
                                  if latencies else "—")))
                        port += 20
                        if port > 65000:
                            port = args.base_port
    except KeyboardInterrupt:
        interrupted = True
        print("\ninterrupted — partial results kept (resume by rerunning)")

    ended = time.time()
    rows = load_all_rows()
    with open(os.path.join(RESULTS, "grpc_sweep_summary.md"), "w") as stream:
        stream.write(render_summary(rows, args))
    render_plots(rows, args)
    write_manifest(args, started, ended, len(rows))
    print("wrote %s and results/grpc_sweep_summary.md" % CSV_PATH)
    if interrupted:
        print("NOTE: sweep incomplete — rerun the same command to resume")
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
