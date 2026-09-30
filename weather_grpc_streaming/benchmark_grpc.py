#!/usr/bin/env python3
"""
Streaming analytics benchmark (Section 2 Q2).

For each configuration (workers x batch size) this starts a coordinator +
workers, replays the dataset as fast as possible through the streaming
client, and records:
    end-to-end time, streaming throughput (records/s), and the latency of
    GetAnalytics queries issued concurrently with ingestion.

Writes results/grpc_benchmark.csv and renders results/grpc_throughput.png +
results/grpc_query_latency.png.

Usage:
    python3 benchmark_grpc.py [--dataset FILE] [--sizes N ...]
                              [--workers W ...] [--batches B ...] [--reps R]

The dataset is generated with the baseline generator when missing
(reproducible: fixed seed). Query-latency measurements issue K probes from
a separate thread during ingestion and report the median.
"""

import argparse
import csv
import os
import signal
import statistics
import subprocess
import sys
import threading
import time

import grpc

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = HERE                                   # section dir (has src/, results/)
REPO = os.path.dirname(ROOT)
SRC = os.path.join(ROOT, "src")
sys.path.insert(0, SRC)

import weather_pb2                            # noqa: E402
import weather_pb2_grpc                       # noqa: E402

GENERATOR = os.path.join(REPO, "weather_baselines", "tools", "gen_dataset.py")
RESULTS = os.path.join(ROOT, "results")


def ensure_dataset(size, stations=500, seed=42):
    os.makedirs("/tmp/opencode", exist_ok=True)
    path = "/tmp/opencode/q8grpc_bench_%d.txt" % size
    if not os.path.exists(path):
        subprocess.run([sys.executable, GENERATOR, "--n", str(size),
                        "--k", "10", "--s", str(stations), "--seed",
                        str(seed), "--out", path], check=True)
    return path


def start_system(port, workers):
    procs = []
    coord_addr = "127.0.0.1:%d" % port
    # stdin=PIPE: the coordinator's CLI loop would exit on EOF (DEVNULL)
    # and shut the server down immediately.
    coordinator = subprocess.Popen(
        [sys.executable, os.path.join(SRC, "server.py"), "0.0.0.0:%d" % port],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL)
    for attempt in range(40):
        try:
            with grpc.insecure_channel(coord_addr) as channel:
                stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)
                stub.GetAnalytics(weather_pb2.AnalyticsRequest(), timeout=0.5)
            break
        except grpc.RpcError:
            time.sleep(0.3)
    else:
        coordinator.kill()
        raise RuntimeError("coordinator did not start on %d" % port)
    for index in range(workers):
        wport = port + 100 + index
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(SRC, "worker.py"),
             "0.0.0.0:%d" % wport, coord_addr,
             "--advertise", "127.0.0.1:%d" % wport],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL))
    with grpc.insecure_channel(coord_addr) as channel:
        stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)
        deadline = time.time() + 20
        while time.time() < deadline:
            if len(stub.GetAnalytics(weather_pb2.AnalyticsRequest()).workers) >= workers:
                break
            time.sleep(0.2)
    channel = grpc.insecure_channel(coord_addr)
    return procs, coord_addr, weather_pb2_grpc.WeatherCoordinatorStub(channel), channel


def stop_system(procs):
    for process in procs:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
    for process in procs:
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
    time.sleep(0.3)


def run_once(coord_addr, dataset, batch_size, query_probes):
    """One replay at max speed; returns (elapsed, recs, query latencies)."""
    channel = grpc.insecure_channel(
        coord_addr, options=[("grpc.max_send_message_length", 64 * 1024 * 1024)])
    stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)

    latencies = []
    stop_probe = threading.Event()

    def prober():
        while not stop_probe.is_set():
            start = time.perf_counter()
            try:
                stub.GetAnalytics(weather_pb2.AnalyticsRequest(), timeout=10)
                latencies.append(time.perf_counter() - start)
            except grpc.RpcError:
                pass
            time.sleep(0.02)

    probe_thread = None
    if query_probes:
        probe_thread = threading.Thread(target=prober, daemon=True)
        probe_thread.start()

    start = time.perf_counter()
    result = subprocess.run(
        [sys.executable, os.path.join(SRC, "client.py"),
         "--dataset", dataset, "--coordinator", coord_addr,
         "--batch-size", str(batch_size), "--quiet"],
        capture_output=True, timeout=600)
    elapsed = time.perf_counter() - start
    stop_probe.set()
    if probe_thread:
        probe_thread.join(timeout=2)
    channel.close()
    if result.returncode != 0:
        raise RuntimeError("client failed: %s" % result.stderr[-500:])
    # The replayed record count is the dataset header's N (first token).
    with open(dataset) as stream:
        records = int(stream.readline().split()[0])
    return elapsed, records, latencies


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sizes", type=int, nargs="+", default=[100000])
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4])
    parser.add_argument("--batches", type=int, nargs="+", default=[1000, 5000])
    parser.add_argument("--reps", type=int, default=3)
    parser.add_argument("--queries", type=int, default=0,
                        help="1: also measure concurrent query latency")
    parser.add_argument("--base-port", type=int, default=50600)
    parser.add_argument("--no-plots", action="store_true",
                        help="Skip generating plots (only write CSV)")
    args = parser.parse_args()

    os.makedirs(RESULTS, exist_ok=True)
    csv_path = os.path.join(RESULTS, "grpc_benchmark.csv")
    rows = []
    port = args.base_port
    for size in args.sizes:
        dataset = ensure_dataset(size)
        for workers in args.workers:
            for batch_size in args.batches:
                for rep in range(args.reps):
                    procs, coord_addr, stub, channel = \
                        start_system(port, workers)
                    try:
                        elapsed, records, latencies = run_once(
                            coord_addr, dataset, batch_size,
                            query_probes=args.queries and rep == args.reps - 1)
                        recs_per_s = records / elapsed if elapsed else 0
                        median_query_ms = (statistics.median(latencies) * 1000
                                           if latencies else "")
                        rows.append({"records": size, "workers": workers,
                                     "batch_size": batch_size,
                                     "rep": rep + 1,
                                     "elapsed_s": "%.3f" % elapsed,
                                     "recs_per_s": "%.0f" % recs_per_s,
                                     "query_latency_ms": (
                                         "%.2f" % median_query_ms
                                         if median_query_ms != "" else "")})
                        print("N=%d W=%d B=%d rep%d: %.2fs  %.0f rec/s  "
                              "query %s ms" %
                              (size, workers, batch_size, rep + 1, elapsed,
                               recs_per_s, median_query_ms))
                    finally:
                        stop_system(procs)
                        channel.close()
                        port += 1

    with open(csv_path, "w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=[
            "records", "workers", "batch_size", "rep", "elapsed_s",
            "recs_per_s", "query_latency_ms"])
        writer.writeheader()
        writer.writerows(rows)
    print("wrote", csv_path)

    # ---- plots (median of reps) ----
    if args.no_plots:
        return

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from collections import defaultdict

        medians = defaultdict(list)
        for row in rows:
            medians[(row["records"], row["workers"], row["batch_size"])
                    ].append(float(row["recs_per_s"]))
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for batch_size in args.batches:
            xs = sorted(args.workers)
            ys = [statistics.median(
                medians[(args.sizes[0], w, batch_size)]) for w in xs]
            ax.plot(xs, ys, marker="o", label="batch=%d" % batch_size)
        ax.set_xlabel("workers")
        ax.set_ylabel("streaming throughput (records/s)")
        ax.set_title("gRPC streaming: throughput vs workers (N=%d)"
                     % args.sizes[0])
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS, "grpc_throughput.png"), dpi=150)

        if args.queries:
            qlats = defaultdict(list)
            for row in rows:
                if row["query_latency_ms"]:
                    qlats[(row["workers"], row["batch_size"])].append(
                        float(row["query_latency_ms"]))
            fig, ax = plt.subplots(figsize=(7, 4.5))
            for batch_size in args.batches:
                xs = sorted(args.workers)
                ys = [statistics.median(
                    qlats.get((w, batch_size), [0])) for w in xs]
                ax.plot(xs, ys, marker="o", label="batch=%d" % batch_size)
            ax.set_xlabel("workers")
            ax.set_ylabel("median GetAnalytics latency (ms)")
            ax.set_title("gRPC streaming: concurrent query latency")
            ax.legend()
            ax.grid(alpha=0.3)
            fig.tight_layout()
            fig.savefig(os.path.join(RESULTS, "grpc_query_latency.png"),
                        dpi=150)
        print("wrote results/grpc_throughput.png")
    except ImportError:
        print("matplotlib unavailable — skipping plots")


if __name__ == "__main__":
    main()
