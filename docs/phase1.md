# Phase 1 — sharded store, routing and scatter-gather

Scope: persistent shard map, shard-key lookup, shard nodes with document
storage, a query router doing direct routing and scatter-gather, and the
baseline latency numbers.  No replication: every chunk has exactly one copy.

## 1. Partitioning

The shard key is `_id` (configurable in `config/cluster.json`).  A key is
hashed to 32 bits with MD5 — truncated and deterministic across processes,
unlike Python's salted `hash()`:

```
hash_key(k) = int(md5(canonical_json(k))[0:4])        ∈ [0, 2^32)
```

The hash space is split into `num_chunks` half-open ranges `[lo, hi)`.  Each
chunk is owned by exactly one shard, and the chunks tile the space with no
gap or overlap — a snapshot that does not tile is rejected on load.

Hashing (rather than ranging on the raw key) buys an even spread: with 20 000
synthetic keys over 4 shards the imbalance is under 2%.  The chunk layer is
what makes Phase 3 possible — migration reassigns a chunk, it never
re-partitions the space.

## 2. The persistent shard map

`data/shardmap.db` is a small SQLite database, the analogue of a MongoDB
config server:

| table | contents |
|---|---|
| `shards` | `shard_id`, host, port, state |
| `chunks` | `chunk_id`, `lo`, `hi`, `owner`, state |
| `meta` | `version`, `shard_key`, `hash_space`, timestamps |

`version` is bumped by every structural change.  The router holds a
`ShardMapCache`: it keeps an immutable `ShardMapSnapshot` and rebuilds it only
when the stored version moves.  A version probe is one indexed read, so
routing stays cheap while a chunk hand-over is picked up on the next request —
this is the hook Phase 3 uses at cutover.

Lookup is a bisect over the chunk lower bounds:

```
shard_for_key(k) = owner( chunks[ bisect_right(lows, hash_key(k)) - 1 ] )
```

O(log C) for C chunks, with no network call.

## 3. Shard nodes

One uvicorn process per shard, each with its own SQLite file:

```
documents(doc_id PK, key_hash, body JSON, version, updated_at)
          index on key_hash
```

`key_hash` is stored alongside the document so a shard can scan, count or
hand over exactly one chunk range without re-hashing every row — again for
Phase 3.  `version` increments on each write and is the basis for Phase 2's
replication stream.

| method | path | meaning |
|---|---|---|
| `GET` | `/health`, `/stats` | liveness, document count, disk size |
| `POST` | `/documents`, `/documents/bulk` | insert (409 on duplicate `_id`) |
| `GET` | `/documents/{id}` | key lookup |
| `PUT` | `/documents/{id}` | update, optional upsert |
| `DELETE` | `/documents/{id}` | delete |
| `POST` | `/query`, `/count` | filter query over this shard only |

Updates take either a replacement document or operators `$set`, `$unset`,
`$inc`, `$mul`, `$push`, `$pull`, on dotted paths.  `_id` is immutable: moving
a document between shards is migration, not an update.

## 4. The filter API

A filter maps dotted field paths to a literal (equality) or an operator
document; clauses are ANDed.

```json
{"city": "Delhi", "age": {"$gte": 30}, "tags": {"$in": ["a","b"]}}
```

Operators: `$eq $ne $gt $gte $lt $lte $in $nin $exists $regex $size $type`,
plus `$and`, `$or`, `$nor`, `$not`.  Queries take `sort`, `limit`, `skip` and
a `fields` projection.

Values of different JSON types are still totally ordered (missing < null <
bool < number < string < array < object), so sorting never fails on a
heterogeneous or absent field — and, importantly, the *router* uses the same
ordering, which is what makes a merge across shards correct.

Filters are validated at the router before any shard is contacted, so a bad
query costs one 400 instead of a fan-out.

## 5. Routing

The router extracts shard-key equality constraints from the filter
(`extract_shard_keys`), descending through `$and`/`$or`:

| what the filter says about `_id` | mode | shards touched |
|---|---|---|
| `{"_id": "u1"}`, `{"_id": {"$eq": …}}` | `direct` | 1 |
| `{"_id": {"$in": [...]}}`, `$or` of equalities | `multi-key` | only the owners |
| nothing, or a range (`$gt`, `$ne`) | `scatter-gather` | all |

An `$or` with one unconstrained branch degrades the whole query to
scatter-gather, which is the conservative answer.

The same analysis runs again *inside* the shard: a filter that pins `_id`
becomes a primary-key `IN` lookup instead of a collection scan.  Without this
a directly routed `/query` was 7× slower than `GET /documents/{id}` for the
same key (18.8 ms vs 2.4 ms at 20 000 documents) — the router picked one
shard, but that shard still read everything it had.  Each query response
reports `examined` so the difference is visible.

### Scatter-gather and the merge

For a fan-out query the router asks every owning shard concurrently
(`asyncio.gather` over a pooled `httpx.AsyncClient`) and merges the replies.

With a sort, each shard returns its own documents already ordered, so the
merge is a k-way heap merge under the shared comparator — O(n log k) and no
re-sort.  The window needs care: with a global `skip`/`limit`, any one shard
could supply the whole of the first `skip + limit` results, so every shard is
asked for `skip + limit` and the router applies the real window after merging.
A single-shard plan is asked for exactly `limit`.

Writes are routed by the shard key in the document.  A bulk insert is grouped
by owning shard first, so N documents cost one request per shard rather than
N requests.

## 6. What Phase 2 receives

* A running 4-shard cluster plus router, started by `scripts/run_cluster.py`.
* The API contracts above; `shardstore/api_models.py` holds the request and
  response models shared by both services.
* `ShardMap.set_chunk_owner()` and the version-cache mechanism, ready for
  ownership epochs.
* `/health` already reports per-shard state and the router degrades to
  `degraded` when a shard is unreachable; a failed shard call surfaces as
  `503 shard <id> unavailable`, which is where failover will hook in.

## 7. Known limits (deliberate, for later phases)

* One copy of each chunk — losing a shard loses its data (Phase 2).
* Chunk ownership is static; nothing splits or moves chunks yet (Phase 3).
* A non-shard-key filter is a full scan on every shard; there are no
  secondary indexes.  This is what makes the scatter-gather cost visible in
  the baseline, which is the point of the measurement.
* The router is a single process and a single point of failure.
