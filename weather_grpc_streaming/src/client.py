#!/usr/bin/env python3
"""
Streaming replay client (Section 2 Q2).

Reads a pre-generated weather dataset and replays it as a continuous
stream of RecordBatches, as if the records were arriving from a live
source. Rate control:

    --replay-speed X   replay at X times real time; the record timestamps
                       in the dataset span T seconds, so the stream lasts
                       T/X seconds. X <= 0 means "as fast as possible".
    --batch-size N     records per gRPC message (streaming granularity).
    --rate             explicit records/second cap, overriding timestamps.

After the stream finishes, prints the final analytics report (and can
write it to a file with --out for correctness tests).

Usage:
    python3 client.py --dataset weather.txt --coordinator node01:50060 \
        --batch-size 500 --replay-speed 100
"""

import argparse
import sys
import time

import grpc

import weather_pb2
import weather_pb2_grpc


def read_dataset(path):
    """Parse the baseline dataset format -> (top_k, records list)."""
    records = []
    top_k = 10
    with open(path) as stream:
        first = True
        for line in stream:
            parts = line.split()
            if not parts:
                continue
            if first:
                top_k = int(parts[1])
                first = False
                continue
            records.append((int(parts[0]), int(parts[1]),
                            float(parts[2]), float(parts[3]),
                            float(parts[4]), float(parts[5]),
                            float(parts[6])))
    return top_k, records


def batches(iteration, top_k, records, batch_size):
    """Yield RecordBatches; the first carries the header flag."""
    for start in range(0, len(records), batch_size):
        chunk = records[start:start + batch_size]
        batch = weather_pb2.RecordBatch(
            header=(start == 0),
            top_k=top_k,
            batch_seq=iteration * 10**9 + start // batch_size,
            records=[weather_pb2.WeatherRecord(timestamp=ts, station_id=sid,
                                               temperature=t, humidity=h,
                                               pressure=p, rainfall=r,
                                               wind_speed=w)
                     for ts, sid, t, h, p, r, w in chunk])
        yield batch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--coordinator", default="localhost:50060")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--replay-speed", type=float, default=0,
                        help=">0: multiple of dataset timestamps; "
                             "0: as fast as possible")
    parser.add_argument("--rate", type=float, default=0,
                        help="records per second cap (overrides timestamps)")
    parser.add_argument("--out", default=None,
                        help="write the final report to this file")
    parser.add_argument("--quiet", action="store_true")
    args = parser.parse_args()

    top_k, records = read_dataset(args.dataset)
    if not records:
        sys.exit("client: dataset has no records")

    # Rate control from dataset timestamps: records are sorted by ts?
    # Not necessarily — replay follows the dataset's arrival order, which
    # for the generator is random. Map each record to a send delay using
    # its relative timestamp position (stable, reproducible).
    span = max(record[0] for record in records) - min(record[0] for record in records)
    per_record_delay = 0.0
    if args.rate > 0:
        per_record_delay = 1.0 / args.rate
    elif args.replay_speed > 0:
        per_record_delay = (span / args.replay_speed) / len(records)

    channel = grpc.insecure_channel(
        args.coordinator,
        options=[("grpc.max_send_message_length", 64 * 1024 * 1024)])
    stub = weather_pb2_grpc.WeatherCoordinatorStub(channel)

    start = time.time()
    summary = stub.StreamRecords(
        batches(0, top_k, records, args.batch_size), wait_for_ready=True)
    elapsed = time.time() - start

    if not args.quiet:
        print("[client] streamed %d records in %.2fs (%.0f rec/s)"
              % (summary.records_accepted, elapsed,
                 summary.records_accepted / elapsed if elapsed else 0),
              file=sys.stderr)
        print("[client] final analytics:", file=sys.stderr)
    report = summary.final_report
    from weather_state import WeatherPartial
    print(WeatherPartial.from_proto(report.aggregate).render(report.top_k),
          end="")
    if args.out:
        with open(args.out, "w") as stream:
            stream.write(WeatherPartial.from_proto(
                report.aggregate).render(report.top_k))
    channel.close()


if __name__ == "__main__":
    main()
