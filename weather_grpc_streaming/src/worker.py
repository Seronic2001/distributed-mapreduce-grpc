#!/usr/bin/env python3
"""
Weather analytics worker (Section 2 Q2).

Each worker is an independent gRPC server holding its own WeatherPartial
state. The coordinator streams RecordBatches over the bidirectional
ProcessBatches RPC; the worker folds every record into its local state and
acks each batch with the batch's delta Partial (streamed back on the same
RPC). The coordinator can alternatively pull state with GetPartial.

Usage:
    python3 worker.py [LISTEN_ADDR] [COORDINATOR_ADDR]

    LISTEN_ADDR      default 0.0.0.0:50061
    COORDINATOR_ADDR if set, the worker self-registers with the
                     coordinator (RegisterWorker) on startup; otherwise
                     pass "worker:host:port" lines to the server's CLI
                     (run_server.sh) or start workers first with
                     self-registration disabled.

Threading: gRPC thread pool delivers concurrent batch streams; a lock
protects the partial so Reset / GetPartial / ProcessBatches stay consistent.
"""

import argparse
import logging
import sys
import threading
from concurrent import futures

import grpc

import weather_pb2
import weather_pb2_grpc
from weather_state import WeatherPartial


class WeatherWorkerServicer(weather_pb2_grpc.WeatherWorkerServicer):
    def __init__(self, endpoint):
        self.endpoint = endpoint
        self.partial = WeatherPartial()
        self.lock = threading.Lock()
        self.batches = 0

    # ---------------- bidi stream: coordinator pushes, worker acks --------
    def ProcessBatches(self, request_iterator, context):
        try:
            for batch in request_iterator:
                delta = WeatherPartial()
                for record in batch.records:
                    delta.update_record(record.timestamp, record.station_id,
                                        record.temperature, record.humidity,
                                        record.pressure, record.rainfall,
                                        record.wind_speed)
                with self.lock:
                    self.partial.merge(delta)
                    self.batches += 1
                    worker_total = self.partial.count
                yield weather_pb2.BatchAck(batch_seq=batch.batch_seq,
                                           worker_total=worker_total,
                                           delta=delta.to_proto())
        except grpc.RpcError:
            # Coordinator went away or reset the stream — a normal event
            # during shutdown; the sender thread reconnects on its own.
            return
        except Exception:
            logging.exception("worker %s: batch stream failed", self.endpoint)
            context.abort(grpc.StatusCode.INTERNAL, "batch stream failed")

    # ---------------- pulled state (alternative sync mode) ----------------
    def GetPartial(self, request, context):
        with self.lock:
            return self.partial.to_proto(weather_pb2.Partial())

    def Reset(self, request, context):
        with self.lock:
            self.partial = WeatherPartial()
            self.batches = 0
        return weather_pb2.Ack(ok=True, message="worker state reset")


def serve(listen_addr):
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=32),
        options=[("grpc.max_receive_message_length", 64 * 1024 * 1024)])
    servicer = WeatherWorkerServicer(listen_addr)
    weather_pb2_grpc.add_WeatherWorkerServicer_to_server(servicer, server)
    server.add_insecure_port(listen_addr)
    server.start()
    logging.info("worker listening on %s", listen_addr)
    return server, servicer


def register_with_coordinator(listen_addr, coordinator_addr, advertise):
    """Advertise this worker to the coordinator (best effort).

    The advertised endpoint must be reachable FROM the coordinator. On a
    multi-node deployment pass --advertise explicitly (e.g. the worker's
    node hostname); 0.0.0.0 is replaced by the coordinator's hostname as a
    same-node convenience default.
    """
    if advertise:
        endpoint = advertise
    else:
        host, _, port = listen_addr.rpartition(":")
        if host in ("0.0.0.0", "", "::"):
            host = coordinator_addr.rpartition(":")[0]
        endpoint = "%s:%s" % (host, port)
    try:
        with grpc.insecure_channel(coordinator_addr) as channel:
            stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)
            reply = stub.RegisterWorker(
                weather_pb2.RegisterRequest(worker_endpoint=endpoint),
                timeout=10)
            logging.info("registered with coordinator %s as %s",
                         coordinator_addr, endpoint)
    except grpc.RpcError as error:
        logging.warning("could not register with %s: %s",
                        coordinator_addr, error.details())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("listen", nargs="?", default="0.0.0.0:50061",
                        help="address to listen on (default 0.0.0.0:50061)")
    parser.add_argument("coordinator", nargs="?", default=None,
                        help="coordinator address for self-registration "
                             "(e.g. node01:50060)")
    parser.add_argument("--advertise", default=None,
                        help="endpoint to register with the coordinator "
                             "(must be reachable from the coordinator; "
                             "default: derived from LISTEN)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%%(asctime)s [worker %s] %%(message)s"
                               % (args.coordinator or args.listen))
    server, _ = serve(args.listen)
    if args.coordinator:
        register_with_coordinator(args.listen, args.coordinator, args.advertise)
    server.wait_for_termination()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)
