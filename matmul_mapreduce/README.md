# Section 1 Q1 — Distributed Matrix Multiplication (Row-Row) with MapReduce

`C = A × B` computed with the **row-row method**: row `i` of `C` is the
weighted sum of the rows of `B`, the weights being the entries of row `i`
of `A`:

```
C_i = a_i,1·B_1,: + a_i,2·B_2,: + ... + a_i,n·B_n,:
```

## MapReduce design

| Requirement | How it is met |
|---|---|
| **Data distribution** | `A` is the distributed input: the framework feeds each mapper only its split of `A` (under Hadoop, HDFS splits; locally, `run_local.sh` splits the file). `B` is **fully replicated** to every mapper: under Hadoop via `-files` (distributed cache, staged into each task's working dir), locally by giving every mapper its own cache copy of `B`. |
| **Map phase** | Each mapper reads `B` once into memory, then processes **its rows of `A` independently** (no inter-worker communication). For each row it computes the finished row `C_i = Σ_k a_i,k · B_k,:` and emits `key = i`, `value = "i:j:c_ij"` for every nonzero cell. |
| **Intermediate data** | At most `m·p` pairs in total (160 000 for 400×400), instead of the `m·n·p` pairs a per-term mapper would emit (6.4·10⁷), so the shuffle stays small. |
| **Shuffle** | The framework groups all pairs with key `i` on one reducer — exactly the data needed to build row `i` of `C`. |
| **Reduce phase** | The reducer sums the values per `(i, j)` and writes row `i` of `C`. With one reduce task the output is the whole matrix `C`. |
| **Combiner** | `combiner.py` sums values per `(i, j)`. For dense `A` rows are already complete, so it does little; for sparse `A` (one `a_i,k` per input line) it merges the partial rows. Its output is valid mapper output, so the reducer is unchanged. |
| **Edge cases** | `m` not divisible by the number of mappers (line-aligned splits with remainder), `m = 1`, `n = 1`, more mappers than rows, skewed shapes, and sparse matrices (zero cells are skipped entirely). |

### Formats

* **dense** — one row per line, space separated (`1 2` / `-1 4`).
* **sparse** — `i<TAB>j<TAB>value` per nonzero cell (0-indexed). Preferred
  under Hadoop: a dense file carries no global row ids and Hadoop Streaming
  does not tell a mapper which rows of `A` it received (a custom InputFormat
  would); sparse records carry their row id inside the record.

## Usage

Local pipeline (simulates split → map → shuffle → combine → shuffle → reduce,
timed):

```bash
bash run_local.sh A.txt B.txt C_output.txt 3        # 3 mappers
COMBINE=0 bash run_local.sh A.txt B.txt C.txt 4     # disable combiner
```

Hadoop 3.3.6 / YARN (one Streaming job; `A`, `B` → HDFS, `B` via `-files`):

```bash
export HADOOP_HOME=...
bash run_hadoop.sh A.txt B.txt C_output.txt
python3 to_dense.py < C_output.txt                  # pretty dense output
```

Slurm (used for all benchmarks, since Hadoop is unavailable on the cluster; same
staging as `run_local.sh`, local background processes without Slurm):

```bash
sbatch matmul_slurm.sh A.txt B.txt C_output.txt 4   # or: bash matmul_slurm.sh ...
sbatch --nodes=4 --ntasks=16 matmul_slurm.sh A.txt B.txt C.txt 16
COMBINE=0 bash matmul_slurm.sh A.txt B.txt C.txt 4  # disable combiner
```

All `R` mappers start as one `srun` step. The `step_layout` helper spreads
them evenly over the job's nodes (e.g. `--nodes=4 --ntasks-per-node=4` for
16 mappers on 4 nodes), and each run prints where the mappers ran, e.g.
`map placement: node01.local x4, node02.local x4, ...`. Shuffle, combine and
reduce then run as single tasks over the shared filesystem.

## Results (cluster job 100460, 4 nodes)

`A` and `B` are 400×400 (`gen_matrices.py --m 400 --n 400 --p 400 --seed 42`);
seconds, from `results/matmul_timings.csv` (last two rows):

| Mappers | map | shuffle | combine | reduce | total |
|---|---|---|---|---|---|
| R = 8 (2 per node) | 2.81 | 0.25 | 0.72 | 0.42 | **4.21** |
| R = 16 (4 per node) | 1.55 | 0.25 | 0.72 | 0.42 | **2.95** |

The map stage scales almost ideally (1.8× from 8 to 16 mappers); the other
stages run as one process each and stay at 1.4 s, which limits the total
speed-up to 1.4×.

Verification oracle (brute force, used by tests):

```bash
python3 verify.py A.txt B.txt            # prints C in A's format
python3 verify.py A.txt B.txt --sparse   # force sparse output
```

Reproducible test matrices:

```bash
python3 gen_matrices.py --m 3 --n 2 --p 3 --seed 1 --out-a A.txt --out-b B.txt
python3 gen_matrices.py --m 1000 --n 800 --p 600 --density 0.1 --sparse ...
```

## Correctness

```bash
bash tests/run_tests.sh        # 29 checks
```

* **Example 1** from the spec (even split, 3 mappers × 1 row) — exact
  expected `C` from the PDF, dense and sparse.
* **Example 2** (uneven split 2+1+1 rows over 3 mappers) — plus R=1.
* **Edge cases**: `m=1`, `n=1` (single column of A × single row of B),
  `1×1`, more mappers than rows.
* **Randomised**: square/tall/wide/sparse configs, each checked
  **byte-for-byte** against the brute-force oracle, with and without the
  combiner (the combiner must not change results).

Entries are integers (generator range −5..5), so summation order cannot
change any output bit.

## Files

| File | Purpose |
|---|---|
| `mapper.py` | Map: rows of `A` × replicated `B` → finished rows of `C` as `(i, i:j:c_ij)` pairs |
| `combiner.py` | Optional pre-aggregation per `(i, j)` (also the Hadoop combiner) |
| `reducer.py` | Reduce: sums values per `(i, j)`, writes `C` |
| `run_local.sh` | Local simulation of the full MapReduce flow (timed stages) |
| `matmul_slurm.sh` | Slurm orchestration (mappers spread over the job's nodes; fallback = local processes) |
| `run_hadoop.sh` | Hadoop Streaming launcher (Hadoop 3.3.6 + YARN) |
| `verify.py` | Brute-force oracle |
| `gen_matrices.py` | Reproducible matrix generator (dense/sparse) |
| `to_sparse.py`, `to_dense.py` | Format converters |
| `tests/run_tests.sh` | The 29-check suite |
| `results/matmul_timings.csv` | Stage timings of every Slurm run (appended per run) |
