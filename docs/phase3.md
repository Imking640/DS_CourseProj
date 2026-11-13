# Phase 3 — online chunk migration

Scope: move one chunk's hash range from one shard to another **while writes
continue**, flip the shard map at cutover so the router follows the chunk,
make sure the destination's follower also receives the moved documents, and
record how many documents moved.

## 1. Why a chunk is the unit of movement

Phase 1 split the 32-bit hash space into chunks and gave each chunk an owner.
Nothing here re-partitions that space: a migration only changes one row's
`owner` in the chunk table. That is what keeps the operation cheap and
reversible, and it is why `shard_for_key` keeps working unchanged throughout —
the bisect is over the same boundaries before and after.

Ownership is recorded against whichever node is *active* for a logical shard,
so a chunk whose owner failed over in Phase 2 is owned by the promoted
follower and can still be migrated. That case is exercised below.

## 2. The protocol

```
donor                                   recipient
  │                                         │
  │ 1. arm  ── start logging writes in      │
  │           [lo, hi) to migration_log     │
  │                                         │
  │ 2. clone ─ paged by _id cursor ────────▶│ ingest (replicates to follower)
  │                                         │
  │ 3. catch-up ─ drain migration_log ─────▶│ ingest
  │    (repeat while writes continue)       │
  │                                         │
  │ 4. freeze ─ refuse *writes* to [lo,hi)  │
  │    drain the tail ─────────────────────▶│ ingest
  │                                         │
  │    ╔═══ shard map: chunk.owner = recipient, version++ ═══╗
  │                                         │
  │ 5. commit ─ drop [lo, hi) locally       │
  │             and on the donor's follower │
```

Steps 1–3 run with the donor fully serving reads and writes. Only step 4
blocks writes, and only for the range being moved — measured at **14–45 ms**
on the runs below.

### Arming before counting

`DocumentStore.arm_range` sets the capture range *and* counts the documents in
it under one lock. A concurrent write therefore either commits before the
count (so the clone sees it) or after (so the changelog catches it). There is
no instant at which a write is in neither.

### Cursor paging, not offsets

The clone pages with `_id > last_seen` rather than `skip`/`limit`. Documents
are being deleted during the copy, and an offset window steps over a document
every time an earlier one disappears underneath it.

### Idempotent shipping

A batch is applied as upsert-by-`_id` plus delete-if-present, so replaying one
after a retry can neither duplicate nor lose a document. This is what lets the
catch-up phase re-send freely.

## 3. The two correctness problems, and how each is solved

### Duplicates: orphan filtering

Between the clone starting and the donor's cleanup, the donor and the
recipient both physically hold the moving documents. A scatter-gather asks
every owner, so without help it would return each of those documents twice.

Every scatter-gather read now carries, per shard, the hash ranges that shard
owns according to the current map (`owned_ranges` on `/query` and `/count`).
A shard returns only documents inside those ranges. Since the chunk table is
a partition, exactly one shard claims any given document — so during the
overlap the donor returns them and after the flip the recipient does, never
both. This is the same idea as MongoDB's shard filtering.

### Lost writes: the block lives in the storage layer

The first implementation checked "is this range frozen?" in the HTTP handler
and then called into the store. The load test immediately found the hole:

```
request passes the freeze check
  → freeze arrives, final drain reports an empty backlog
    → map flips, donor deletes the range
      → the request's write lands, in a range that was just dropped
```

That silently destroyed acknowledged writes — the harness reported 5 lost and
1 stale document on its first run, which is exactly the failure this phase
exists to rule out.

The check now lives in `DocumentStore`, evaluated while holding the same lock
that performs the write (`RangeNotOwned`). Taking that lock in
`set_blocked_ranges` means that once freezing returns, every in-flight write
has already committed *and* been logged, and every later write is refused.
A refused write becomes a `409` carrying a retry marker; the router reloads
the shard map, re-plans and retries, so the request follows the chunk to its
new owner instead of failing.

## 4. Two bugs this phase surfaced in the Phase 2 code

Both only appear under concurrency, which is why the migration load test found
them and the Phase 2 functional tests did not.

* **The node-state SQLite connection was shared across threads with no lock.**
  FastAPI runs sync endpoints in a threadpool, and concurrent `execute()` on
  one `sqlite3` connection interleaves cursor state, so `state("role")`
  intermittently returned `None` and the follower raised a 500. The primary
  turned that into "follower did not acknowledge" and failed the write. Now
  serialised by a lock.
* **A new `httpx.AsyncClient` was built per replicated write.** Under load
  this opened a fresh connection to the follower every time and exhausted
  sockets. Replaced with one pooled client per process.

Together these took the load test from 26 failed writes per run to zero, and
roughly 8× the accepted write throughput during a migration.

## 5. Running it

```bash
.venv/bin/python scripts/run_cluster.py --reset
.venv/bin/python scripts/seed_data.py --count 20000

.venv/bin/python scripts/migrate_chunk.py --list              # chunk table
.venv/bin/python scripts/migrate_chunk.py --chunk 3 --to shard-0

# migrate while writers hammer the range, then check for loss/duplication
.venv/bin/python scripts/migrate_under_load.py --chunk 5 --to shard-2 --writers 6
```

## 6. Measured results

4 shards + 4 followers, 20 000 seeded documents, 6 concurrent writers aimed at
the chunk being moved. Full output in `results/`.

### A quiescent move, for reference

Chunk 3 (`shard-3` → `shard-0`): **2 551 documents moved**, map version 1 → 2,
cutover 44.6 ms, total 329 ms, verified.

### Migration under live writes

Chunk 5, `shard-1` → `shard-2`, with 6 writers aimed at the moving range:

| | |
|---|---|
| documents moved | **2 943** |
| copied in the clone phase | 2 907 |
| live changes carried over | 129 (14 of them deletes) |
| writes accepted during the migration | 2 298 |
| write errors | 0 |
| cutover write-freeze | **35.9 ms** |
| **lost documents** | **0** |
| **duplicate documents** | **0** |
| resurrected deletes | 0 |
| stale values | 0 |

The harness classes each write as acknowledged, rejected or *indeterminate*,
and excludes only indeterminate ones from the comparison — a write whose
outcome the client never learned proves nothing in either direction. On the
recorded run there were none, so all 559 tracked documents were checked
strictly.

### Destination replica

After the chunk 5 move, comparing that range on all four nodes involved:

| node | documents in range | digest |
|---|---:|---|
| `shard-2` (destination primary) | 3 061 | `ad396643ab64…` |
| `shard-2-follower` | 3 061 | `ad396643ab64…` |
| `shard-1` (donor primary) | 0 | — |
| `shard-1-follower` | 0 | — |

The destination's follower is byte-identical to its primary: ingest batches go
through the ordinary replication path, so the follower receives them like any
other write. The donor's cleanup is replicated too, so the donor's follower
sheds the range rather than keeping orphans.

### Migration after a failover

`shard-0`'s primary was killed, its follower was promoted (recovery 48.1 ms),
and chunk 4 — then owned by `shard-0-follower` — was migrated to `shard-3`:

```
documents moved : 2509      map version : 4 -> 5
cutover         : 13.7 ms   verified    : True
cluster total   : 20559 documents before and after
```

Migration and failover compose because a chunk's owner is whichever node is
*active* for that logical shard, which the shard map already tracks.

## 7. What Phase 4 receives

* `scripts/migrate_chunk.py` for a single move and `scripts/migrate_under_load.py`
  for the move-under-writes demonstration; both append JSON to `results/`.
* `shardstore/migration.py` — `ChunkMigrator` is importable, so a demo script
  can drive a migration inline and read the `MigrationReport` back.
* 19 migration tests in `tests/test_migration.py` (112 across the suite).
* The chunk `state` column and `/internal/migration/state` for observing a
  migration in progress.

## 8. Known limits (deliberate)

* One chunk moves at a time per donor; a second concurrent donation from the
  same shard is refused rather than silently losing capture.
* Chunks never split or merge, and nothing balances automatically — a move is
  always explicitly requested.
* The changelog is kept in the donor's SQLite file and cleared on commit or
  abort. If the donor dies mid-migration the migration is abandoned; the map
  is untouched, so the chunk simply stays where it was (or follows the donor's
  failover).
* The cutover flips one chunk row and bumps the map version. Routers notice on
  their next request; an in-flight request is covered by the retry path, not by
  a distributed lock.
