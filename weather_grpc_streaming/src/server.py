#!/usr/bin/env python3
"""
WeatherCoordinator — streaming weather analytics server (Section 2 Q2).

Responsibilities
----------------
* Worker registry: workers either self-register (RegisterWorker) or are
  added via the CLI ("worker host:port"). Registry changes are picked up
  between batches, so workers can join mid-stream.
* Ingestion: StreamRecords (client-streaming) accepts RecordBatches from
  replay clients and distributes them round-robin to the connected workers
  over the bidirectional ProcessBatches RPC. Each worker stream is owned by
  one sender thread with a bounded queue; the per-batch delta Partial in the
  ack is merged into the coordinator's mirror of that worker's state, so the
  coordinator's combined analytics stay consistent with the workers without
  polling.
* Queries: GetAnalytics merges the live worker snapshots (lazily, guarded
  by the global lock) into a fresh global Partial and returns it with
  throughput counters. Queries are valid at any time — during ingestion,
  before any data, and with zero workers.
* Subscriptions: SubscribeUpdates pushes AnalyticsReport snapshots on a
  timer (server-streaming) for the CLI dashboard.

CLI (stdin commands, for the demo / operations):
    worker <host:port>     add a worker endpoint
    status                 print the worker table
    reset                  clear all analytics state (coordinator + workers)
    quit                   shut down

Usage:
    python3 server.py [LISTEN_ADDR]        (default 0.0.0.0:50060)
"""

import argparse
import logging
import queue
import sys
import threading
import time
from concurrent import futures

import grpc

import weather_pb2
import weather_pb2_grpc
from weather_state import WeatherPartial

DEFAULT_SNAPSHOT_INTERVAL = 1.0
BATCH_QUEUE_DEPTH = 64


class WorkerHandle:
    """One connected worker: a dedicated bidi stream + sender thread."""

    def __init__(self, endpoint, coordinator):
        self.endpoint = endpoint
        self.coordinator = coordinator
        self.records_processed = 0
        self.active = False
        self.queue = queue.Queue(maxsize=BATCH_QUEUE_DEPTH)
        self.lock = threading.Lock()          # protects records_processed
        self.partial = WeatherPartial()       # coordinator's mirror state
        self._stop = threading.Event()
        self.pending = 0                      # enqueued but not yet acked
        self._drained = threading.Condition()

    def wait_until_drained(self, timeout):
        """Block until every enqueued batch has been acked (or timeout)."""
        deadline = time.time() + timeout
        with self._drained:
            while self.pending > 0 and not self._stop.is_set():
                remaining = deadline - time.time()
                if remaining <= 0:
                    return False
                self._drained.wait(remaining)
            return self.pending == 0

    def reset_state(self):
        """Reset mirror state and pending counts without disconnecting."""
        with self.lock:
            self.records_processed = 0
            self.partial = WeatherPartial()
        with self._drained:
            self.pending = 0
            while not self.queue.empty():
                try:
                    self.queue.get_nowait()
                except queue.Empty:
                    break
            self._drained.notify_all()

    # ------------------------------------------------- sender thread ----
    def start(self):
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="worker-%s" % self.endpoint)
        self.thread.start()

    def _run(self):
        while not self._stop.is_set():
            channel = grpc.insecure_channel(
                self.endpoint,
                options=[("grpc.enable_retries", 0)])
            stub = weather_pb2_grpc.WeatherWorkerStub(channel)
            try:
                # One long-lived bidi call; the iterator ends only on
                # shutdown (or stream recreation after a worker restart).
                stream = stub.ProcessBatches(self._batches(), wait_for_ready=True)
                for ack in stream:
                    with self.lock:
                        self.records_processed = ack.worker_total
                        self.partial.merge(
                            WeatherPartial.from_proto(ack.delta))
                    with self._drained:
                        self.pending -= 1
                        self._drained.notify_all()
                logging.warning("worker %s stream ended", self.endpoint)
            except grpc.RpcError as error:
                if self._stop.is_set():
                    break
                logging.warning("worker %s: %s", self.endpoint,
                                error.code() if error.code() else error)
                with self._drained:
                    self.pending = 0
                    self._drained.notify_all()
                time.sleep(1.0)   # retry loop — workers may join late
            finally:
                try:
                    channel.close()
                except Exception:
                    pass
            if self._stop.wait(1.0):
                break

    def _batches(self):
        while True:
            try:
                item = self.queue.get(timeout=0.25)
            except queue.Empty:
                if self._stop.is_set():
                    return
                continue
            if item is None:
                return
            yield item

    def send(self, batch, timeout=30.0):
        with self._drained:
            self.pending += 1
        try:
            self.queue.put(batch, timeout=timeout)
        except queue.Full:
            with self._drained:
                self.pending -= 1
            raise

    def stop(self):
        self._stop.set()
        try:
            self.queue.put(None, timeout=1)
        except queue.Full:
            pass


class WeatherCoordinatorServicer(weather_pb2_grpc.WeatherCoordinatorServicer):
    def __init__(self):
        self.global_lock = threading.Lock()   # worker table + counters
        self.workers = {}                     # endpoint -> WorkerHandle
        self.round_robin = []
        self.rr_index = 0
        self.top_k = 10
        self.records_received = 0
        self.batches_received = 0
        self.started_at = time.time()
        self.ingest_started = None        # first batch of the current run
        self.ingest_elapsed = 0.0         # duration of the last/active run
        self.active_until = 0.0           # last data arrival (+grace)
        self.snapshot_interval = DEFAULT_SNAPSHOT_INTERVAL

    # ------------------------------------------------ worker management --
    def add_worker(self, endpoint):
        with self.global_lock:
            if endpoint in self.workers:
                return False, "already registered"
            handle = WorkerHandle(endpoint, self)
            self.workers[endpoint] = handle
            self.round_robin.append(handle)
            handle.start()
            return True, "worker %s added" % endpoint

    def RegisterWorker(self, request, context):
        ok, message = self.add_worker(request.worker_endpoint)
        logging.info("register worker %s: %s", request.worker_endpoint, message)
        return weather_pb2.Ack(ok=ok, message=message)

    # ----------------------------------------------------- ingestion -----
    def StreamRecords(self, request_iterator, context):
        for batch in request_iterator:
            if self.ingest_started is None:
                self.ingest_started = time.time()
            if batch.header:
                with self.global_lock:
                    self.top_k = batch.top_k or self.top_k
            records = len(batch.records)
            if records:
                self._distribute(batch)
            with self.global_lock:
                self.records_received += records
                self.batches_received += 1
                self.active_until = time.time() + 2.0
        with self.global_lock:
            if self.ingest_started is not None:
                self.ingest_elapsed = time.time() - self.ingest_started
            handles = list(self.workers.values())
        # Wait until the workers have actually processed every enqueued
        # batch, so the summary and any subsequent query reflect the full
        # stream (the ack deltas keep the coordinator's mirror exact).
        for handle in handles:
            handle.wait_until_drained(timeout=300.0)
        return self._ingest_summary()

    def _distribute(self, batch):
        """Round-robin one batch over the live worker streams.

        A full worker queue means that worker is backed up; we try the
        other workers and, if all queues stay full, wait briefly and
        retry — this is the flow-control backpressure toward the client.
        """
        deadline = time.time() + 120.0
        while True:
            with self.global_lock:
                if not self.round_robin:
                    raise RuntimeError("no workers connected")
                count = len(self.round_robin)
                handles = [self.round_robin[(self.rr_index + offset) % count]
                           for offset in range(count)]
                self.rr_index += 1
            for handle in handles:
                try:
                    handle.send(batch, timeout=0.05)
                    return
                except queue.Full:
                    continue
            if time.time() > deadline:
                raise RuntimeError("all worker queues are full")
            time.sleep(0.05)

    def _ingest_summary(self):
        with self.global_lock:
            report = self._build_report_locked()
            records, batches = self.records_received, self.batches_received
            elapsed = self.ingest_elapsed or (time.time() - self.started_at)
        return weather_pb2.IngestSummary(
            records_accepted=records,
            batches_accepted=batches,
            elapsed_seconds=elapsed,
            records_per_second=(records / elapsed if elapsed > 0 else 0.0),
            final_report=report)

    # ------------------------------------------------------- queries -----
    def GetAnalytics(self, request, context):
        with self.global_lock:
            return self._build_report_locked()

    def _build_report_locked(self):
        now = time.time()
        global_state = WeatherPartial()
        worker_infos = []
        for handle in self.round_robin:
            with handle.lock:
                worker_infos.append(weather_pb2.WorkerInfo(
                    endpoint=handle.endpoint,
                    records_processed=handle.records_processed))
                global_state.merge(handle.partial)
        active_workers = sum(1 for handle in self.round_robin
                             if handle.thread.is_alive())
        elapsed = now - self.started_at
        return weather_pb2.AnalyticsReport(
            aggregate=global_state.to_proto(weather_pb2.Partial()),
            top_k=self.top_k,
            records_received=self.records_received,
            batches_received=self.batches_received,
            elapsed_seconds=elapsed,
            records_per_second=(self.records_received / elapsed
                                if elapsed > 0 else 0.0),
            ingestion_active=now < self.active_until,
            active_workers=active_workers,
            workers=worker_infos,
            report_unix_time=int(now))

    def SubscribeUpdates(self, request, context):
        interval = request.interval_seconds or self.snapshot_interval
        last_sent = None
        while context.is_active():
            with self.global_lock:
                report = self._build_report_locked()
            payload = report.SerializeToString()
            if payload != last_sent:      # skip identical snapshots
                yield report
                last_sent = payload
            time.sleep(interval)

    def Reset(self, request, context):
        with self.global_lock:
            for handle in self.workers.values():
                try:
                    with grpc.insecure_channel(handle.endpoint) as channel:
                        stub = weather_pb2_grpc.WeatherWorkerStub(channel)
                        stub.Reset(weather_pb2.ResetRequest(), timeout=5)
                except grpc.RpcError as error:
                    logging.warning("reset %s failed: %s", handle.endpoint,
                                    error.code())
                handle.reset_state()
            self.records_received = 0
            self.batches_received = 0
            self.ingest_started = None
            self.ingest_elapsed = 0.0
            self.started_at = time.time()
        return weather_pb2.Ack(ok=True, message="coordinator and workers reset")

    # -------------------------------------------------- dashboard sync ---
    def snapshot_interval_value(self):
        with self.global_lock:
            return self.snapshot_interval


def serve(listen_addr):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=64),
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)])
    servicer = WeatherCoordinatorServicer()
    weather_pb2_grpc.add_WeatherCoordinatorServicer_to_server(servicer, server)
    server.add_insecure_port(listen_addr)
    server.start()
    logging.info("coordinator listening on %s", listen_addr)
    return server, servicer


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("listen", nargs="?", default="0.0.0.0:50060",
                        help="listen address (default 0.0.0.0:50060)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [coordinator] %(message)s")
    server, servicer = serve(args.listen)

    # ---- operations CLI ----
    print("commands: worker <host:port> | status | reset | quit",
          file=sys.stderr)
    for line in sys.stdin:
        parts = line.split()
        if not parts:
            continue
        command = parts[0].lower()
        if command == "worker" and len(parts) == 2:
            ok, message = servicer.add_worker(parts[1])
            print("[coordinator] %s" % message, file=sys.stderr)
        elif command == "status":
            with servicer.global_lock:
                for handle in servicer.round_robin:
                    print("[coordinator] %-24s records=%d" %
                          (handle.endpoint, handle.records_processed),
                          file=sys.stderr)
        elif command == "reset":
            servicer.Reset(weather_pb2.ResetRequest(), None)
            print("[coordinator] reset done", file=sys.stderr)
        elif command == "quit":
            break
    server.stop(grace=1).wait()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
