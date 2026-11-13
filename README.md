# MongoDB-Inspired Sharded Document Store

A JSON-document store split across independent shards, fronted by a query
router that reads a persistent shard map.  Course project, built in phases:

| Phase | Scope | Status |
|---|---|---|
| 1 | Shard map, shard nodes, query router, scatter-gather, baseline latency | **done** |
| 2 | Followers, replication, ownership epochs, failover | **done** |
| 3 | Live chunk migration between shards | **done** |
| 4 | End-to-end correctness tests, full benchmark run, demo script | planned |

## At a glance

```
                       ┌───────────────────────┐
   client ──HTTP──▶    │   query router :9000  │
                       └───────────┬───────────┘
            reads │ shard map (SQLite: chunks, replicas, epochs, version)
                  ▼
     ┌────────────┬────────────┬────────────┬────────────┐
     │ shard-0    │ shard-1    │ shard-2    │ shard-3    │  primaries
     │ :9101      │ :9102      │ :9103      │ :9104      │  :910x
     └─────┬──────┴─────┬──────┴─────┬──────┴─────┬──────┘
           │ replicate  │            │            │
     ┌─────▼──────┬─────▼──────┬─────▼──────┬─────▼──────┐
     │ follower   │ follower   │ follower   │ follower   │  followers
     │ :9201      │ :9202      │ :9203      │ :9204      │  :920x
     └────────────┴────────────┴────────────┴────────────┘
       chunks 0,4   chunks 1,5   chunks 2,6   chunks 3,7
```

* The **shard map** partitions a 32-bit hash space into *chunks*; each chunk
  is owned by one shard.  It lives in its own SQLite file and carries a
  version that every mutation bumps, plus replica membership and ownership
  epochs.
* The **router** hashes the shard key, bisects the chunk table and forwards
  the request.  If a query does not pin the shard key it is **scatter-gathered**
  to every shard and the replies are merge-sorted into one result.
* A **shard** is a separate OS process with its own SQLite database.  Each has
  a follower that receives writes synchronously; losing a primary promotes the
  follower and bumps its epoch.
* A **chunk can be migrated** between shards while writes continue; the map
  flips at cutover and routers follow it.

Every router response carries a `routing` block saying which path was taken,
so direct, scatter-gather, primary and follower reads are easy to tell apart.

## Quick start

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

.venv/bin/python scripts/run_cluster.py --reset     # 4 primaries + 4 followers + router
.venv/bin/python scripts/seed_data.py --count 20000 # load documents
.venv/bin/python scripts/bench_phase1.py            # baseline latency
.venv/bin/python scripts/run_cluster.py --stop
```

Migrate a chunk between shards, with and without concurrent writes:

```bash
.venv/bin/python scripts/migrate_chunk.py --list
.venv/bin/python scripts/migrate_chunk.py --chunk 3 --to shard-0
.venv/bin/python scripts/migrate_under_load.py --chunk 5 --to shard-2 --writers 6
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

curl 'localhost:9000/documents/u1?read_preference=follower'
                                                     # potentially stale read
```

## Layout

```
shardstore/
  hashing.py      32-bit shard-key hashing and even range splitting
  shardmap.py     persistent chunk table, replicas, epochs, version cache
  config.py       cluster topology from config/cluster.json
  filters.py      filter language, ordering, shard-key extraction
  storage.py      per-shard SQLite store, orphan filtering, migration log
  shard_node.py   shard HTTP service (replication + migration endpoints)
  routing.py      route planning and result merging
  router.py       router HTTP service (failover + migration retry)
  migration.py    the chunk migration protocol
  workload.py     synthetic documents and latency statistics
scripts/
  init_shardmap.py      bootstrap the shard map
  run_cluster.py        start/stop every shard process and the router
  seed_data.py          bulk-load synthetic documents
  bench_phase1.py       direct vs scatter-gather baseline
  migrate_chunk.py      move one chunk to another shard
  migrate_under_load.py move a chunk under live writes and verify
docs/
  phase1.md          architecture, API contract, design notes
  phase1_baseline.md measured baseline latency
  phase2.md          replication, read preference, epochs, failover
  phase3.md          online chunk migration
tests/              unit and end-to-end tests  (pytest)
results/            recorded benchmark and migration measurements
```

## Tests

```bash
.venv/bin/python -m pytest
```

112 tests. The end-to-end and migration tests mount the shard apps into the
router's HTTP client, so they exercise the real request path — planning,
one HTTP call per shard, merge, and a whole migration — without binding ports.
