#!/usr/bin/env python3
"""
CLI dashboard (Section 2 Q2).

Subscribes to the coordinator's SubscribeUpdates stream and renders live
analytics while ingestion is still running: a per-worker load table, the
combined counters, and the full 19-line baseline report. A --watch N
mode polls GetAnalytics instead (useful to demonstrate concurrent query
clients); --once prints one snapshot and exits (used by tests).

Usage:
    python3 dashboard.py --coordinator node01:50060 [--interval 1.0]
    python3 dashboard.py --coordinator node01:50060 --watch 0.2
    python3 dashboard.py --coordinator node01:50060 --once [--csv file]
"""

import argparse
import csv
import sys
import time

import grpc

import weather_pb2
import weather_pb2_grpc
from weather_state import WeatherPartial


def render(report):
    state = WeatherPartial.from_proto(report.aggregate)
    lines = []
    lines.append("=" * 62)
    lines.append(" WEATHER ANALYTICS DASHBOARD   %s" %
                 time.strftime("%H:%M:%S", time.localtime(report.report_unix_time)))
    lines.append("=" * 62)
    lines.append(" ingestion: %s   workers: %d   records: %d   batches: %d"
                 % ("ACTIVE" if report.ingestion_active else "idle",
                    report.active_workers, report.records_received,
                    report.batches_received))
    lines.append(" throughput: %.0f rec/s over %.1fs"
                 % (report.records_per_second, report.elapsed_seconds))
    lines.append("-" * 62)
    lines.append(" %-22s %12s" % ("worker", "records"))
    for worker in report.workers:
        lines.append(" %-22s %12d" % (worker.endpoint,
                                      worker.records_processed))
    lines.append("-" * 62)
    lines.append(state.render(report.top_k).rstrip("\n"))
    lines.append("=" * 62)
    return "\n".join(lines)


def snapshot_csv(report, path):
    """Append one row of counters (for throughput experiments)."""
    import os
    write_header = not os.path.exists(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="") as stream:
        writer = csv.writer(stream)
        if write_header:
            writer.writerow(["unix_time", "records", "batches",
                             "records_per_second", "elapsed_s",
                             "active_workers"])
        writer.writerow([report.report_unix_time, report.records_received,
                         report.batches_received,
                         "%.1f" % report.records_per_second,
                         "%.3f" % report.elapsed_seconds,
                         report.active_workers])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coordinator", default="localhost:50060")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="snapshot cadence for subscribe mode")
    parser.add_argument("--watch", type=float, default=0,
                        help="poll GetAnalytics every N seconds instead of "
                             "subscribing (concurrent query client demo)")
    parser.add_argument("--once", action="store_true",
                        help="print one snapshot and exit")
    parser.add_argument("--csv", default=None,
                        help="append counter rows to this CSV (with --once "
                             "or --watch)")
    args = parser.parse_args()

    channel = grpc.insecure_channel(args.coordinator)
    stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)

    if args.once:
        report = stub.GetAnalytics(weather_pb2.AnalyticsRequest())
        print(render(report))
        if args.csv:
            snapshot_csv(report, args.csv)
        return

    if args.watch > 0:
        try:
            while True:
                report = stub.GetAnalytics(weather_pb2.AnalyticsRequest())
                print(render(report))
                if args.csv:
                    snapshot_csv(report, args.csv)
                time.sleep(args.watch)
        except KeyboardInterrupt:
            return

    # ---- default: server-streaming subscription ----
    try:
        for report in stub.SubscribeUpdates(
                weather_pb2.SubscribeRequest(interval_seconds=args.interval)):
            print(render(report))
            print()
            if args.csv:
                snapshot_csv(report, args.csv)
    except KeyboardInterrupt:
        pass
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.CANCELLED:
            raise


if __name__ == "__main__":
    main()
