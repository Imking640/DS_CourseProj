# Phase 1 baseline: direct routing vs scatter-gather

Generated 2026-10-30 22:33:11 on Linux (20 CPUs, Python 3.12.3).

4 shards / 8 chunks, 20,000 documents, no replication, 300 sequential requests per workload, split over 3 interleaved rounds.

Scatter-gather timings are CPU-bound (every shard parses and filters its
whole collection) and so move with machine load; the direct-path numbers
are stable to within a few percent across runs.

## Latency

| workload | routing | fanout | n | mean | p50 | p95 | p99 | ops/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `key_lookup` | direct | 1 | 300 | 4.63 | 4.39 | 7.76 | 8.74 | 216 |
| `point_query` | direct | 1 | 300 | 3.99 | 3.68 | 5.70 | 7.09 | 251 |
| `insert` | direct | 1 | 300 | 3.93 | 3.48 | 5.97 | 10.88 | 254 |
| `update` | direct | 1 | 300 | 3.94 | 3.58 | 5.77 | 6.90 | 254 |
| `multi_key_query` | multi-key | <=4 | 300 | 5.08 | 4.60 | 7.89 | 9.92 | 197 |
| `filter_selective` | scatter-gather | 4 | 300 | 63.36 | 62.82 | 113.17 | 125.62 | 16 |
| `filter_broad` | scatter-gather | 4 | 300 | 63.28 | 62.94 | 97.58 | 114.70 | 16 |
| `filter_sorted_limit` | scatter-gather | 4 | 300 | 127.90 | 69.19 | 306.77 | 321.54 | 8 |
| `count` | scatter-gather | 4 | 300 | 61.87 | 45.94 | 188.93 | 216.01 | 16 |

## Throughput (concurrency 16)

| workload | routing | fanout | n | mean | p50 | p95 | p99 | ops/s |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| `key_lookup` | direct | 1 | 300 | 34.43 | 23.11 | 106.67 | 173.84 | 450 |
| `filter_broad` | scatter-gather | 4 | 300 | 712.94 | 735.47 | 796.93 | 843.35 | 22 |

## Headline

- Direct key lookup p50: **4.39 ms**
- Scatter-gather filter p50: **62.94 ms** (14.3x the direct path, fanout 4)
