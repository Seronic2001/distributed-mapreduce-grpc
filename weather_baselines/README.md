# Q8 — Large-Scale Weather and Environmental Data Analytics

Sequential and MPI implementations of the weather-data analytics task,
plus a reproducible dataset generator.

## Files

| File | Purpose |
|------|---------|
| `src/q8_sequential.cpp` | Sequential implementation (also the verification oracle) |
| `src/q8_mpi.cpp` | MPI implementation |
| `tools/gen_dataset.py` | Dataset generator with a fixed PRNG seed |
| `tests/sample.txt`, `tests/sample_expected.txt` | Small hand-verified sample with all 19 output lines checked manually |
| `tests/run_tests.sh` | Compares sequential and MPI outputs for P=1,2,4,8 on the sample and generated datasets |
| `benchmark.sh` | Sequential vs MPI timings over increasing input sizes |

## Input format

First line: `N K S`.
Next N lines: `timestamp station_id temperature humidity pressure rainfall wind_speed`.

## Output format

Exactly as specified in the assignment (TOTAL_MEASUREMENTS ... TOP_STATIONS).
All floating-point values are printed with exactly six digits after the
decimal point. Tie-breaking follows the assignment/clarifications:

- Top-K stations: decreasing measurement count, then increasing station ID.
- Hottest/coldest: larger/smaller temperature first, then smaller
  timestamp, then smaller station ID.
- Busiest interval: interval id = timestamp / 60; ties resolved to the
  smaller interval id.
- If fewer than K distinct stations appear in the data, only the
  available stations are printed under TOP_STATIONS.

## Build

```
mkdir -p build
g++ -O2 -std=c++17 -o build/q8_sequential src/q8_sequential.cpp
mpicxx -O2 -std=c++17 -o build/q8_mpi src/q8_mpi.cpp
```

The Homework 3 tests and benchmarks expect the binaries in `build/`
(`build/q8_sequential` is the oracle for both Section 2 problems).

## Run

```
./build/q8_sequential tests/sample.txt
mpiexec -n 4 ./build/q8_mpi tests/sample.txt
```

## MPI design

1. **Distribution** — rank 0 reads/parses the whole file and scatters an
   equal number of records to every rank via contiguous `MPI_BYTE` blocks
   with `MPI_Scatterv` (avoiding datatype serialization overhead). Remainder
   records go one-per-rank to the lowest ranks.
2. **Local aggregation** — each rank computes partial sums/min/max for
   temperature/humidity/pressure/rainfall/wind, extreme-event count, a
   local hottest/coldest candidate, its own busiest-interval histogram
   (hash map), and per-station accumulators (arrays of size S).
3. **Reduction** —
   - sums/counts/per-station arrays via `MPI_Reduce` with `MPI_SUM`;
   - min/max via `MPI_Reduce` with `MPI_MIN`/`MPI_MAX` (+inf/-inf sentinels);
   - hottest/coldest candidates gathered from all ranks and merged on
     rank 0 using the exact tie-breaking rules;
   - interval histograms serialised into key/value arrays and combined
     on rank 0 via `MPI_Gatherv`.
4. Rank 0 prints the report; timing breakdown goes to stderr.

## Correctness verification

```
bash tests/run_tests.sh
```

The hand-computed sample is compared byte-for-byte against the expected
report for the sequential version and P=1,2,4,8. Generated datasets are
then cross-checked: the MPI output must match the sequential output
exactly. This is reliable because the generator emits values on a 0.5
grid, which is exactly representable in binary floating point, so
floating-point addition order cannot change any result bit.

Generate reproducible datasets:

```
python3 tools/gen_dataset.py --n 1000000 --k 10 --s 500 --seed 42 --out weather_1m.txt
```

The generator uses Python's Mersenne Twister seeded deterministically;
values: timestamp in [0, tmax], station uniform over S stations,
temperature [-30,50], humidity [0,100], pressure [950,1050],
rainfall [0,50], wind speed [0,40] — all on a 0.5 grid.

## Benchmarking

```
bash benchmark.sh              # writes results/benchmark.csv
```

Benchmarks the sequential program and the MPI program at P=1,2,4,8 over
several input sizes (configurable at the top). The stderr phase timings
allow communication-vs-computation analysis. The plots in `results/` are the
baseline speedup/efficiency figures. For Homework 3, the MPI vs MapReduce
comparison is run by `../weather_mapreduce/benchmark.sh`, which reuses
these binaries.
