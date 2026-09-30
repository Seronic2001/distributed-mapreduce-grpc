# Section 2 Q1 — Q8 Weather Analytics with Hadoop MapReduce

The baseline real-world problem (large-scale weather/environment data
analytics) reimplemented with **Hadoop MapReduce** (Hadoop 3.3.6 + YARN,
**C++** programs through **Hadoop Streaming**), plus a comparison against
the baseline MPI implementation.

## MapReduce decomposition

One MapReduce stage is sufficient because every one of the 19 output lines
is a **commutative, associative merge** of per-record partials:

```
records ──map──▶ partial blocks ──combine──▶ one partial per mapper
                 ──shuffle──▶ single reducer ──▶ 19-line report
```

* **Mapper** (`src/q8_mapper.cpp`) — parses its split of the dataset and
  aggregates locally into a `Partial` (sums, minima, maxima, extreme-event
  count, hottest/coldest candidate with the assignment's tie-breaking, a
  busiest-interval histogram keyed by `timestamp/60`, and per-station
  accumulators). It flushes one wire-format block every `Q8MR_FLUSH`
  records (default 200 000), so memory is bounded and the combiner has real
  work. The mapper that reads the file header (`N K S`) emits one `meta`
  pair carrying `K`. **No inter-task communication** — each map task works
  only on its split, mirroring the MPI ranks' local aggregation.
* **Combiner** (`src/q8_combiner.cpp`) — merges the mapper's blocks into
  one per node, so **per-record data never crosses the network**; only
  aggregates travel through the shuffle.
* **Reducer** (`src/q8_reducer.cpp`, exactly **one** per job) — merges the
  per-mapper partials into the global aggregate and prints the 19-line
  report, **byte-identical** to the sequential and MPI outputs of
  baseline. A single global report requires a single reducer because
  every wire key is a global tag.

Wire format (one line per partial component; see `src/common.h`):
`count/sums/mins/maxs/extreme/hot/cold/iv/st` — every tag merges
commutatively (add / min / max / compare-and-keep), so arrival order is
irrelevant. Doubles use `%.17g` + `strtod` for exact binary64 round-trips
across stage boundaries.

Tie-breaking (same as baseline): top-K stations by decreasing count then
increasing id; hottest/coldest by temperature, then smaller timestamp, then
smaller station id; busiest interval by larger count then smaller interval
id; `TOP_STATIONS` lists only stations present in the data.

## Build

```bash
make        # builds build/q8_mapper, build/q8_combiner, build/q8_reducer
```

## Run

Local simulation of the Streaming flow (split → map → combine → shuffle →
reduce, timed):

```bash
bash run_local.sh dataset.txt report.txt 4          # 4 mappers
COMBINE=0 bash run_local.sh dataset.txt report.txt 2
```

Hadoop 3.3.6 + YARN:

```bash
export HADOOP_HOME=...
bash run_hadoop.sh dataset.txt report.txt [num_map_tasks_hint]
```

Under Hadoop the dataset goes to HDFS (the distributed input); HDFS block size
controls map parallelism (`num_map_tasks_hint` lowers `dfs.blocksize`
accordingly); the C++ binaries travel to the nodes with `-files`.

Slurm (used for all benchmarks, since Hadoop is unavailable on the cluster; same
staging as `run_local.sh`, local background processes without Slurm):

```bash
sbatch run_slurm.sh dataset.txt report.txt 8        # or: bash run_slurm.sh ...
sbatch --nodes=4 --ntasks=16 run_slurm.sh dataset.txt report.txt 16
COMBINE=0 bash run_slurm.sh dataset.txt report.txt 4   # disable combiner
```

The mappers run as one `srun` step and the combiners as another; the
`step_layout` helper spreads each step evenly over the job's nodes (at most one
node per task), and each run prints where the map tasks ran, e.g.
`map placement: node01.local x2, node02.local x2, ...`.

## Correctness verification

```bash
make test                    # or: bash tests/run_tests.sh
```

24 checks, all **byte-for-byte**:

* the hand-verified sample from baseline (`weather_baselines/tests/`) for
  M = 1, 2, 4, 8 mappers, without the combiner, and with a forced tiny
  mapper flush interval (many partial blocks per mapper);
* six generated datasets (`weather_baselines/tools/gen_dataset.py`, fixed seed)
  against the baseline sequential program — the same oracle used for the
  MPI tests.

Parity argument: the generator emits values on a 0.5 grid (exactly
representable in binary64), so floating-point addition order cannot change
any result bit — any mismatch indicates a real bug.

## Benchmarks — MPI vs MapReduce

```bash
bash benchmark.sh            # sequential + MPI + MapReduce (run_slurm.sh inside a Slurm job, run_local.sh otherwise)
HADOOP=1 bash benchmark.sh   # also real Hadoop Streaming rows (needs HADOOP_HOME + YARN)
python3 plots.py             # results/total_time.png, results/speedup.png
```

* `results/comparison.csv` — one row per configuration: engine
  (`sequential`, `mpi`, `mapreduce`, and `hadoop-streaming` with `HADOOP=1`),
  workers, MapReduce stage times, total time (median of 3 runs).
* `results/summary.md` — the same numbers as tables per input size.

Datasets are reproducible (`gen_dataset.py --n N --k 10 --s 500 --seed 42`),
sizes 10⁴ / 10⁵ / 10⁶ records. Mapper and MPI process counts are 1–16 inside
a Slurm job with at least 16 tasks, otherwise 1–8 (override with `MAPPERS=`
and `MPS=`). MPI rows need `mpicxx`/`mpiexec` and are skipped with a warning
when they are missing.

### Results (cluster job 100460, 4 nodes × 4 tasks)

Total seconds, median of 3 runs (stage times are in `results/summary.md`):

| N | sequential | best MPI | best MapReduce |
|---|---|---|---|
| 10⁴ | 0.0039 | 0.0021 (P=1) | 0.219 (M=1) |
| 10⁵ | 0.0133 | 0.0156 (P=1) | 0.301 (M=2) |
| 10⁶ | 0.1064 | 0.1307 (P=2) | 0.583 (M=4) |

* **Neither parallel version beats the sequential program** at these sizes:
  the analytics are one cheap pass over the data, so start-up and data
  movement cost more than the parallel work saves. MapReduce is 5.5× slower
  than sequential at 10⁶.
* **MapReduce has a fixed cost of 0.2–0.5 s** (starting `srun` steps and
  processes, staging files), which is almost the whole run at 10⁴; only the
  map stage grows with N.
* **MapReduce is fastest with one task per node.** At 10⁶ the map stage drops
  from 1.12 s (M=1) to 0.41 s (M=4, one task on each node). With 2 or more
  tasks per node (M ≥ 8) every step starts about 0.1 s slower (visible in the
  combine step: 0.08–0.10 s for M ≤ 4, 0.20–0.22 s for M ≥ 8), so M=8 and
  M=16 are slower overall.
* **MPI is fastest with P ≤ 2** and slows down most from P=4 to P=8, the first
  configuration that needs more than one node (4 tasks per node) — most likely
  the cost of scattering records between nodes (MPI rank placement was not
  logged).
* **Only aggregates cross the shuffle**, so the reduce stage takes
  0.06–0.11 s for every N and M.

The report (`report/report.pdf`, Section 3) discusses memory use,
programming effort, flexibility and fault tolerance as well.

## Files

| File | Purpose |
|---|---|
| `src/common.h` | Shared `Partial` aggregation + wire format + report printer |
| `src/q8_mapper.cpp` | Streaming mapper (local aggregation, block flushes) |
| `src/q8_combiner.cpp` | Streaming combiner (per-node pre-merge) |
| `src/q8_reducer.cpp` | Single reducer (global merge + 19-line report) |
| `run_local.sh` | Local Hadoop-flow simulation with stage timings |
| `run_hadoop.sh` | Hadoop Streaming launcher (HDFS + YARN) |
| `run_slurm.sh` | Slurm orchestration (map/combine steps spread over the job's nodes; fallback = local processes) |
| `tests/run_tests.sh` | 24 byte-for-byte checks vs the sequential oracle |
| `benchmark.sh` | Sequential vs MPI vs MapReduce comparison harness |
| `plots.py` | total-time and speedup plots from `comparison.csv` |
