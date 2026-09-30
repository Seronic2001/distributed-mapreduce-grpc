# gRPC weather streaming — sweep summary

Throughput = records/s, median of 3 rep(s). Speedup is vs W=1 for the same batch size. Query latency = median GetAnalytics latency measured by a concurrent probe during ingestion (only when --queries).

## N = 100,000 records

| workers | batch | throughput rec/s | elapsed s | speedup vs W=1 | query ms |
|---|---|---|---|---|---|
| 1 | 1000 | 90564 | 1.104 | 1.00x | — |
| 1 | 4000 | 107013 | 0.934 | 1.00x | — |
| 1 | 16000 | 100595 | 0.994 | 1.00x | — |
| 2 | 1000 | 110942 | 0.901 | 1.23x | — |
| 2 | 4000 | 113779 | 0.879 | 1.06x | — |
| 2 | 16000 | 88328 | 1.132 | 0.88x | — |
| 4 | 1000 | 82132 | 1.218 | 0.91x | — |
| 4 | 4000 | 95714 | 1.045 | 0.89x | — |
| 4 | 16000 | 79443 | 1.259 | 0.79x | — |
| 8 | 1000 | 73857 | 1.354 | 0.82x | — |
| 8 | 4000 | 75519 | 1.324 | 0.71x | — |
| 8 | 16000 | 72273 | 1.384 | 0.72x | — |

## N = 1,000,000 records

| workers | batch | throughput rec/s | elapsed s | speedup vs W=1 | query ms |
|---|---|---|---|---|---|
| 1 | 1000 | 120532 | 8.297 | 1.00x | — |
| 1 | 4000 | 156166 | 6.403 | 1.00x | — |
| 1 | 16000 | 166086 | 6.021 | 1.00x | — |
| 2 | 1000 | 156667 | 6.383 | 1.30x | — |
| 2 | 4000 | 169011 | 5.917 | 1.08x | — |
| 2 | 16000 | 158607 | 6.305 | 0.95x | — |
| 4 | 1000 | 139989 | 7.143 | 1.16x | — |
| 4 | 4000 | 148073 | 6.753 | 0.95x | — |
| 4 | 16000 | 143325 | 6.977 | 0.86x | — |
| 8 | 1000 | 123881 | 8.072 | 1.03x | — |
| 8 | 4000 | 161979 | 6.174 | 1.04x | — |
| 8 | 16000 | 165092 | 6.057 | 0.99x | — |

## Best configuration per size

- N=100,000: **W=2, batch=4000** → 113779 rec/s
- N=1,000,000: **W=2, batch=4000** → 169011 rec/s
