# Distributed Analytics with MapReduce & gRPC

One analytics problem and one collaboration problem, each solved with the
distributed paradigm that fits it best — and every result **verified
byte-for-byte against a sequential oracle**.

| Module | What it is | Paradigm |
|---|---|---|
| [`matmul_mapreduce/`](matmul_mapreduce) | Matrix multiplication C = A×B (row-row), dense + sparse | MapReduce (Hadoop Streaming, Python) |
| [`weather_mapreduce/`](weather_mapreduce) | 19-metric weather analytics over 1M records, batch | MapReduce (Hadoop Streaming, C++17) |
| [`weather_grpc_streaming/`](weather_grpc_streaming) | The same analytics as a live stream with mid-ingestion queries | gRPC: coordinator + worker pool, bidirectional streaming |
| [`collab_docs_grpc/`](collab_docs_grpc) | Multi-client collaborative document editor with live subscriptions | gRPC: unary + server streaming |
| [`weather_baselines/`](weather_baselines) | Sequential + MPI implementations and dataset generator (correctness oracle and performance baseline) | C++17, OpenMPI |
| [`analysis/`](analysis) | Benchmark figure generation from committed result CSVs | Python / Matplotlib |

## Highlights

- **Single-stage MapReduce by construction.** All 19 analytics reduce to
  commutative/associative merges of per-record partials, so the batch job needs
  one MapReduce stage with one reducer and **no raw record crosses the shuffle** —
  only aggregates. Doubles round-trip exactly (`%.17g` / `strtod`).
- **Real-time gRPC pipeline: ~123k records/s** (peak 169k rec/s at N=10⁶,
  2 workers, batch 4000) with analytics queries answered in **2–4.5 ms
  mid-ingestion**. Workers ack each batch with a state delta so the coordinator
  answers queries without a worker round-trip; bounded queues give backpressure
  with zero record loss.
- **Same answer in three paradigms.** Sequential, MPI, MapReduce and gRPC
  streaming produce byte-identical reports across worker counts, batch sizes and
  mapper counts.
- **Concurrency done deliberately.** Per-document locking with per-subscriber
  bounded queues: 8-way edit races lose no edits and slow subscribers never
  block editors.
- **Reproducible benchmarking.** Slurm multi-node drivers (task placement
  logged), resume-aware gRPC sweep harness, environment + dataset SHA-256
  manifest.
- **66 automated checks across 4 suites** (29 + 24 + 6 + 13), all starting real
  servers on loopback ports.

---

## 1. Prerequisites

```bash
python3 --version                 # 3.8+ — matrix and all gRPC code
pip install grpcio grpcio-tools   # gRPC runtime + protoc plugin
pip install rich                  # docs interactive client
g++ --version                     # C++17 — weather-MR mapper/combiner/reducer
pip install matplotlib            # only for regenerating plots/figures
```

Cluster only: `mpicxx`/`mpiexec` (`module load hpcx-2.7.0/hpcx-ompi`) for the
MPI rows of the weather-MR benchmark; Hadoop 3.3.6 + YARN for `run_hadoop.sh`
(Hadoop was not available on the cluster, so all MapReduce results come from the
Slurm drivers).

## 2. Verify everything (one command per section)

```bash
# Build the sequential baseline first — both weather suites use it as the oracle
mkdir -p weather_baselines/build
g++ -O2 -std=c++17 -o weather_baselines/build/q8_sequential weather_baselines/src/q8_sequential.cpp

# Matrix — 29 checks: PDF examples (even + uneven split), m=1 / n=1 / 1x1,
#          more mappers than rows, random dense/sparse vs brute-force oracle,
#          each with and without the combiner
(cd matmul_mapreduce       && bash tests/run_tests.sh)

# Weather MapReduce — 24 checks: hand-verified sample for M in {1,2,4,8}, no combiner,
#          tiny flush interval, 6 generated datasets vs the sequential oracle
(cd weather_mapreduce   && make && bash tests/run_tests.sh)

# Weather gRPC — 6 checks: final report byte-identical to the oracle for 4
#          worker/batch configurations, queries during ingestion, Reset
(cd weather_grpc_streaming && python3 tests/run_tests.py)

# Docs gRPC — 13 checks: create/open/edit/subscribe, error codes, 200
#          simultaneous same-position edits, subscribers under load, CLI run
(cd collab_docs_grpc    && python3 tests/run_tests.py)
```

Every suite must end with all checks passing. The gRPC suites start real
servers on local ports (weather-gRPC: 50160–50350, moving to the next port if one is
busy; docs: 50601).

## 3. Quickstart — each problem

### Matrix — matrix multiplication (Row-Row MapReduce)

```bash
cd matmul_mapreduce
python3 gen_matrices.py --m 3 --n 2 --p 3 --seed 1 --out-a /tmp/A.txt --out-b /tmp/B.txt
bash run_local.sh /tmp/A.txt /tmp/B.txt /tmp/C.txt 3     # 3 mappers
python3 verify.py /tmp/A.txt /tmp/B.txt > /tmp/C_ref.txt
diff /tmp/C.txt /tmp/C_ref.txt                            # must be empty
```

Each mapper holds its rows of A plus a full copy of B and emits finished rows
of C; the reducer assembles them. Cluster runs: `matmul_slurm.sh` (Slurm) or
`run_hadoop.sh A B OUT` (Hadoop Streaming).

### Weather MapReduce — weather analytics, MapReduce (batch)

```bash
cd weather_mapreduce
make
python3 ../weather_baselines/tools/gen_dataset.py --n 100000 --k 10 --s 500 --seed 42 --out /tmp/weather.txt
bash run_local.sh /tmp/weather.txt /tmp/report_mr.txt 8   # 8 mappers
../weather_baselines/build/q8_sequential /tmp/weather.txt > /tmp/report_seq.txt   # built in step 2
diff /tmp/report_mr.txt /tmp/report_seq.txt               # must be empty
```

One MapReduce stage: mappers aggregate their split, a combiner merges each
mapper's output, one reducer merges everything and prints the 19-line report.
MPI vs MapReduce benchmark: `bash benchmark.sh` (on the cluster).

### Weather gRPC — weather analytics, gRPC streaming (real time)

```bash
cd weather_grpc_streaming
python3 src/server.py 0.0.0.0:50060 &                     # coordinator
python3 src/worker.py 0.0.0.0:50061 localhost:50060 &     # repeat per worker (new port each)
python3 src/client.py --dataset /tmp/weather.txt --coordinator localhost:50060 --batch-size 4000
python3 src/dashboard.py --coordinator localhost:50060    # live view while the client runs
```

The client streams batches (`--batch-size`, optional `--rate` or
`--replay-speed`); the dashboard and `GetAnalytics` work during ingestion.
Stop with `pkill -f "src/(server|worker).py"`. Benchmark sweep:

```bash
python3 sweep_grpc.py --sizes 100000 1000000 --workers 1 2 4 8 \
        --batches 1000 4000 16000 --reps 3        # add --queries for query latency
```

Latest sweep (local WSL2, 8 vCPUs): best is 2 workers with batch 4000 —
113.8k records/s at N=10⁵ and 169.0k records/s at N=10⁶.

### Docs gRPC — collaborative document editing (gRPC)

```bash
cd collab_docs_grpc
python3 src/server.py localhost:50051     # terminal 1
python3 src/client.py localhost:50051     # terminals 2 and 3
```

Client commands: `create NAME ["CONTENT"]`, `open NAME`, `edit NAME POS "TEXT"`,
`subscribe NAME`, `unsubscribe NAME`, `exit`. Subscribed clients receive
edits from other clients automatically. `python3 demo_transcript.py` records a
two-client session to `demo_transcript.txt`.

The generated `*_pb2.py` stubs are included; regenerate them only after
editing a `.proto` (`python3 -m grpc_tools.protoc -Iproto --python_out=src
--grpc_python_out=src proto/<name>.proto`).

## 4. Cluster workflow (`run_cluster.sh`)

`run_cluster.sh` syncs the code to a Slurm cluster (set `CLUSTER_USER` and `CLUSTER_HOST` env vars),
submits the benchmark job, waits for it, downloads the results and redraws the
figures:

```bash
./run_cluster.sh setup-ssh      # one-time: install an SSH key on the cluster
./run_cluster.sh                # push -> sbatch slurm_bench_multinode.sh (4 nodes x 4 tasks) -> wait -> fetch -> plot
./run_cluster.sh 2              # same with --nodes=2 (8 tasks)

./run_cluster.sh push           # sync code to ~/mapreduce_grpc_suite
./run_cluster.sh submit [script]  # push + sbatch (default: slurm_bench.sh)
./run_cluster.sh status         # squeue for this user
./run_cluster.sh logs           # show the latest benchmark log
./run_cluster.sh fetch          # download CSVs and logs, then redraw figures
./run_cluster.sh plot           # redraw figures only (report/make_figures.py)
./run_cluster.sh demo-grpc      # gRPC demo across cluster nodes
```

`slurm_bench_multinode.sh` runs the matrix and weather-MR test suites and
benchmarks (matrix multiplication with R=8 and R=16 mappers; sequential vs MPI
vs MapReduce for N = 10⁴, 10⁵, 10⁶ with 1–16 processes/tasks). The MapReduce
drivers spread their tasks evenly over the job's nodes and log where each map
task ran (`map placement: node01.local x4, ...` in `bench_multinode_<job>.log`).
The cluster's Python has no gRPC package, so the gRPC steps are skipped there
and the gRPC benchmarks were run locally.

## 5. Where the numbers live

| Results | File | Measured on |
|---|---|---|
| Matrix matrix multiplication | `matmul_mapreduce/results/matmul_timings.csv` (last two rows) | cluster job 100460 |
| Weather MR MPI vs MapReduce | `weather_mapreduce/results/comparison.csv`, `summary.md` | cluster job 100460 |
| Weather gRPC gRPC sweep | `weather_grpc_streaming/results/grpc_sweep.csv`, `grpc_sweep_summary.md`, `grpc_sweep_manifest.json` | local WSL2, 8 vCPUs |
| Docs demo | `collab_docs_grpc/demo_transcript.txt` | local |
| Job log (incl. task placement) | `bench_multinode_100460.log` | cluster job 100460 |

## 6. Design summary (details in each README and the report)

- **Matrix** — A is split by rows, B is replicated to every mapper (distributed
  cache). Each mapper computes whole rows `C_i = Σ_k a_ik · B_k` and emits one
  pair per cell, so intermediate data is m·p pairs instead of m·n·p. Dense and
  sparse inputs; dense mode passes each chunk its first-row offset (the
  InputFormat's job in real Hadoop).
- **Weather MR** — all 19 analytics are order-independent merges (add / min / max /
  keep-better), so one MapReduce stage with one reducer suffices and no raw
  record crosses the shuffle; `%.17g` keeps doubles exact between stages.
- **Weather gRPC** — the same mergeable aggregate: any batch→worker distribution gives
  the exact sequential result. Workers reply to each batch with the change it
  made, so the coordinator keeps an up-to-date copy of every worker and answers
  queries without contacting them. Bounded queues slow the client down instead
  of dropping records.
- **Docs** — one lock per document: each edit is applied, versioned and queued
  to every subscriber while the lock is held, so edits never corrupt the text
  and subscribers receive updates in version order. Per-subscriber queues keep
  slow clients from blocking editors (no OT/CRDT — a deliberate simplification).
