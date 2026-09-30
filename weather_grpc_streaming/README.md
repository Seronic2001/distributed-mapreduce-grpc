# Section 2 Q2 — Q8 Weather Analytics as Real-Time Streaming (gRPC)

The same baseline weather problem, now treated as a **continuous stream**:
records are replayed from a pre-generated dataset, processed continuously
by multiple analytics workers over gRPC, and queried by a CLI dashboard
**while ingestion is still running**. Final analytics match the baseline
sequential program exactly.

## Architecture

```
 replay client ──StreamRecords (client-streaming)──▶ WeatherCoordinator
                                                        │  bidi ProcessBatches
                                                        ▼  (round-robin batches)
                                            worker 1 .. worker W  (independent
                                            gRPC servers, own state)
                                                        ▲ ack deltas / GetPartial
 dashboard ──SubscribeUpdates / GetAnalytics───────────▶ coordinator
```

* **WeatherCoordinator** (`src/server.py`) — accepts the replay stream,
  distributes batches **round-robin** to the workers over one long-lived
  bidirectional RPC per worker, combines worker state, answers queries.
  Workers may register themselves (`RegisterWorker`) or be added from the
  server CLI (`worker host:port`), including mid-stream.
* **WeatherWorker** (`src/worker.py`) — an independent gRPC server holding
  a `WeatherPartial`; folds every incoming record into local state and
  acknowledges each batch with that batch's **delta Partial**.
* **Replay client** (`src/client.py`) — streams `RecordBatch` messages with
  configurable granularity (`--batch-size`) and pace (`--replay-speed`
  relative to dataset timestamps, `--rate` rec/s, or max speed).
* **Dashboard** (`src/dashboard.py`) — CLI; subscribes to
  `SubscribeUpdates` (server-streaming snapshots) and renders per-worker
  load, throughput counters and the full 19-line report live. `--watch N`
  polls `GetAnalytics` (concurrent query clients), `--once --csv f` dumps
  one snapshot for experiments.

## Why this design

* **Mergeable state (`src/weather_state.py`)** — the analytics algebra is
  the same commutative/associative merge as the MapReduce job (counts/sums
  add, minima/min, maxima/max, hottest/coldest by the assignment's
  tie-break, interval and per-station histograms add). Because order never
  matters, any batch distribution over any number of workers yields
  **exactly** the sequential result, and queries can merge the current
  worker partials at any instant for a consistent snapshot.
* **Pull vs push**: the coordinator keeps a mirror of each worker's state
  from the ack deltas (push), so query answers need no worker round-trips;
  `GetPartial` (pull) exists as the alternative sync mode and for
  reconciliation.
* **Backpressure**: each worker stream has a bounded queue. When a worker
  is slow the coordinator routes to another worker; when all are full it
  pauses the client's stream (flow control toward the replay source) —
  nothing is dropped while workers stay up (batches already sent to a worker
  whose stream fails are not re-sent).
* **Consistency of concurrent updates/queries**: worker state is guarded by
  a per-worker lock; coordinator counters/tables by a global lock; queries
  build a fresh merged snapshot under those locks.

## Proto interface (proto/weather.proto)

| RPC | Type | Purpose |
|---|---|---|
| `WeatherCoordinator.StreamRecords` | client-streaming | replay ingestion |
| `WeatherCoordinator.GetAnalytics` | unary | current analytics (anytime) |
| `WeatherCoordinator.SubscribeUpdates` | server-streaming | dashboard snapshots |
| `WeatherCoordinator.RegisterWorker` | unary | worker self-registration |
| `WeatherWorker.ProcessBatches` | bidirectional | coordinator → worker batches, worker → coordinator per-batch acks with delta partials |
| `WeatherWorker.GetPartial` / `Reset` | unary | pulled state / reset |

`Partial` is the single mergeable message carried in acks, reports, and
worker state — one type everywhere keeps the algebra uniform.

## Usage

```bash
python3 -m grpc_tools.protoc -Iproto --python_out=src --grpc_python_out=src proto/weather.proto

# terminal 1: workers (one process per worker)
python3 src/worker.py 0.0.0.0:50061 localhost:50060 --advertise localhost:50061
python3 src/worker.py 0.0.0.0:50062 localhost:50060 --advertise localhost:50062

# terminal 2: coordinator (CLI: worker <addr> | status | reset | quit)
python3 src/server.py 0.0.0.0:50060

# terminal 3: replay the dataset as a live stream
python3 src/client.py --dataset weather_1m.txt --coordinator localhost:50060 \
        --batch-size 2000 --replay-speed 50

# terminal 4: live dashboard (works while the stream is running)
python3 src/dashboard.py --coordinator localhost:50060 --interval 1.0
python3 src/dashboard.py --coordinator localhost:50060 --once --csv results/live.csv
```

Multi-node (RCE cluster): run `server.py` on node01, workers on node02..k
(`python3 src/worker.py 0.0.0.0:50061 node01:50060 --advertise node02:50061`),
client and dashboard anywhere (`node01:50060`).

Datasets: `weather_baselines/tools/gen_dataset.py --n 1000000 --k 10 --s 500
--seed 42 --out weather_1m.txt` (reproducible).

## Correctness verification

```bash
python3 tests/run_tests.py
```

* final analytics **byte-for-byte equal to the sequential oracle**
  (`weather_baselines/build/q8_sequential`) for W ∈ {1, 2, 4} workers and batch
  sizes 500 / 997 / 1 000 / 100 000 — the distribution strategy cannot
  change the result because merging is order-independent;
* **mid-stream queries**: `GetAnalytics` is called repeatedly while a
  rate-limited stream is still being ingested; every call must succeed and
  the reports must show records arriving (`midstream_queries`);
* `Reset` clears coordinator and worker state while keeping the workers
  connected (`reset`).

The tests need the baseline oracle binary: `g++ -O2 -std=c++17 -o
../weather_baselines/build/q8_sequential ../weather_baselines/src/q8_sequential.cpp`.

## Performance evaluation

The full grid sweep (resume-aware; rerun the same command after an
interruption):

```bash
python3 sweep_grpc.py --sizes 100000 1000000 --workers 1 2 4 8 \
        --batches 1000 4000 16000 --reps 3            # add --queries for query latency
```

* `results/grpc_sweep.csv` — one row per run: size, workers, batch, elapsed
  time, records/s (and query latency with `--queries`);
* `results/grpc_sweep_summary.md` — medians and speed-up vs one worker;
* `results/grpc_sweep_manifest.json` — grid, software versions, dataset SHA-256;
* `results/grpc_sweep_throughput.png`.

`benchmark_grpc.py` is a smaller single-grid benchmark
(`results/grpc_benchmark.csv`, used as a quick smoke test).

### Results (latest sweep, local WSL2, 8 vCPUs, max-speed replay)

Throughput in thousand records/s, median of 3 runs:

N = 10⁵:

| W | batch 1000 | batch 4000 | batch 16000 |
|---|---|---|---|
| 1 | 90.6 | 107.0 | 100.6 |
| 2 | 110.9 | **113.8** | 88.3 |
| 4 | 82.1 | 95.7 | 79.4 |
| 8 | 73.9 | 75.5 | 72.3 |

N = 10⁶:

| W | batch 1000 | batch 4000 | batch 16000 |
|---|---|---|---|
| 1 | 120.5 | 156.2 | 166.1 |
| 2 | 156.7 | **169.0** | 158.6 |
| 4 | 140.0 | 148.1 | 143.3 |
| 8 | 123.9 | 162.0 | 165.1 |

* **Best: 2 workers, batch 4000** at both sizes (169.0k records/s at 10⁶).
* **Throughput tops out at about 165–170k records/s.** One worker with
  batch 16000 already reaches 166.1k, so the single Python client and
  coordinator (which decode, re-encode and merge every batch) are the likely
  limit, not the workers.
* **Extra workers help only with small batches** (batch 1000: +23% at 10⁵,
  +30% at 10⁶ from 1 to 2 workers). More than 2 workers is slower: each adds a
  process, a stream and a coordinator thread on the same 8 CPUs, which hurts
  most in the ~1 s runs at 10⁵.
* **Batch size:** 1000 → 4000 records per message gives +18% (10⁵) and +30%
  (10⁶) with one worker; 16000 changes little. Batch 4000 is the best or
  within 6% of the best everywhere.
* Query latency, concurrent query clients, rate-limited replay and CPU/memory
  were not measured in this sweep (`--queries`, several
  `dashboard.py --watch` clients and `client.py --rate` support them).

## Files

| File | Purpose |
|---|---|
| `proto/weather.proto` | Service + message definitions |
| `src/weather_state.py` | Mergeable `WeatherPartial` + 19-line report renderer |
| `src/server.py` | Coordinator server (registry, streaming, queries) |
| `src/worker.py` | Analytics worker (bidi stream, ack deltas) |
| `src/client.py` | Streaming client (replay rate, batching) |
| `src/dashboard.py` | CLI dashboard (GetAnalytics & SubscribeUpdates) |
| `tests/run_tests.py` | Automated correctness suite (6 checks) |
| `sweep_grpc.py` | Full workers × batch × size sweep (CSV, summary, plot, manifest) |
| `benchmark_grpc.py` | Smaller throughput / latency benchmark + plots |
