# MongoDB-Inspired Sharded Document Store

A JSON-document store split across independent shards, fronted by a query
router that reads a persistent shard map.  Course project, built in phases:

| Phase | Scope | Status |
|---|---|---|
| 1 | Shard map, shard nodes, query router, scatter-gather, baseline latency | **done** |
| 2 | Followers, replication, ownership epochs, failover | planned |
| 3 | Live chunk migration between shards | planned |
| 4 | End-to-end correctness tests, full benchmark run, demo script | planned |

## Phase 1 at a glance

```
                       ┌───────────────────────┐
   client ──HTTP──▶    │   query router :9000  │
                       └───────────┬───────────┘
                     reads │ shard map (SQLite, versioned)
                           ▼
        ┌────────────┬────────────┬────────────┬────────────┐
        │ shard-0    │ shard-1    │ shard-2    │ shard-3    │
        │ :9101      │ :9102      │ :9103      │ :9104      │
        │ SQLite     │ SQLite     │ SQLite     │ SQLite     │
        └────────────┴────────────┴────────────┴────────────┘
          chunks 0,4    chunks 1,5   chunks 2,6   chunks 3,7
```

* The **shard map** partitions a 32-bit hash space into *chunks*; each chunk
  is owned by one shard.  It lives in its own SQLite file and carries a
  version that every mutation bumps.
* The **router** hashes the shard key, bisects the chunk table and forwards
  the request.  If a query does not pin the shard key it is **scatter-gathered**
  to every shard and the replies are merge-sorted into one result.
* A **shard** is a separate OS process with its own SQLite database.  It knows
  nothing about routing.

Every router response carries a `routing` block saying which path was taken,
so direct and scatter-gather requests are easy to tell apart.

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python scripts/run_cluster.py --reset     # 4 shards + router
.venv/bin/python scripts/seed_data.py --count 10000 # load documents
.venv/bin/python scripts/bench_phase1.py            # baseline latency
.venv/bin/python scripts/run_cluster.py --stop
```

```bash
curl -XPOST localhost:9000/documents \
  -H 'content-type: application/json' \
  -d '{"document":{"_id":"u1","city":"Delhi","age":30}}'

curl localhost:9000/documents/u1                     # direct, one shard

curl -XPOST localhost:9000/query \
  -H 'content-type: application/json' \
  -d '{"filter":{"city":"Delhi","age":{"$gte":25}},"sort":{"age":-1},"limit":10}'
                                                     # scatter-gather, merged
```

## Layout

```
shardstore/
  hashing.py      32-bit shard-key hashing and even range splitting
  shardmap.py     persistent chunk table, snapshots, version cache
  config.py       cluster topology from config/cluster.json
  filters.py      filter language, ordering, shard-key extraction
  storage.py      per-shard SQLite document store
  shard_node.py   shard HTTP service
  routing.py      route planning and result merging
  router.py       router HTTP service
  workload.py     synthetic documents and latency statistics
scripts/
  init_shardmap.py  bootstrap the shard map
  run_cluster.py    start/stop every shard process and the router
  seed_data.py      bulk-load synthetic documents
  bench_phase1.py   direct vs scatter-gather baseline
docs/
  phase1.md          architecture, API contract, design notes
  phase1_baseline.md measured baseline latency
tests/              unit and end-to-end tests  (pytest)
```

## Tests

```bash
.venv/bin/python -m pytest
```

The end-to-end tests mount the shard apps into the router's HTTP client, so
they exercise the real request path without binding ports.
