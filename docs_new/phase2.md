# Phase 2: replication and failover

## What is implemented

- Each logical shard has one primary process and one follower process.
- Primaries synchronously replicate insert, bulk insert, update, and delete operations. A write is acknowledged only after the follower acknowledges it.
- Followers accept replicated writes only at their current ownership epoch.
- Reads default to the active primary. `read_preference: "follower"` (or `?read_preference=follower` for key lookup) reads from the follower when it is online and identifies the source in the response routing metadata.
- The persistent shard map stores primary/follower membership, active owner, and epoch.
- When the router detects an unavailable active primary, it promotes its follower, increments the epoch in the persistent map, and retries the request.
- A former primary checks the shared shard map before writes, so an old epoch cannot write after promotion.
- `/health` reports `failover_count` and `last_recovery_ms`.

## Start

From the project root:

```bash
.venv/bin/python scripts/run_cluster.py --reset
```

`--reset` deletes the primary and follower databases. Do not use it if you need to preserve data. Without `--reset`, the launcher upgrades a Phase 1 shard map to replica-aware metadata while retaining primary document databases, then copies each primary's contents to its follower.

## Write and verify replication

```bash
curl -X POST localhost:9000/documents \
  -H 'content-type: application/json' \
  -d '{"document":{"_id":"phase2-demo","city":"Delhi","n":1}}'
```

The response tells you which shard owns the key. Its follower is on the corresponding port 9201-9204. For example, if the owner is `shard-0`, inspect the follower with:

```bash
curl localhost:9201/documents/phase2-demo
```

Test update replication:

```bash
curl -X PUT localhost:9000/documents/phase2-demo \
  -H 'content-type: application/json' \
  -d '{"update":{"$inc":{"n":4}}}'
```

## Primary versus follower reads

Primary read (default):

```bash
curl 'localhost:9000/documents/phase2-demo'
```

Potentially stale follower read:

```bash
curl 'localhost:9000/documents/phase2-demo?read_preference=follower'
```

For filter queries, include `"read_preference":"follower"` in the JSON body sent to `/query` or `/count`. The response routing block reports `read_preference` and `read_source`.

## Demonstrate failover

1. Insert and update a document, then note the primary shard ID in the response.
2. Kill that primary process. For example, to kill port 9101 on macOS:

   ```bash
   kill -9 $(lsof -ti tcp:9101)
   ```

3. Read the document again through the router. The router should promote the follower and retry the request.
4. Check the new owner and epoch:

   ```bash
   curl localhost:9000/shardmap
   curl localhost:9000/health
   ```

   The affected replica entry should show the follower as `active_id`, with epoch 2. `/health` reports `last_recovery_ms`.

5. A write sent directly to the old primary with its old epoch should be rejected with HTTP 409 because the persistent map no longer names it as active owner.

## Important scope limits

- This is a single-follower educational implementation, not a production consensus protocol. The router and shard processes share the local SQLite shard-map file for fencing, which is appropriate for the local multi-process course demo but is not a distributed consensus service.
- A promoted follower remains primary; automatic failback, rejoining the old primary, and a second promotion are not implemented in Phase 2.
- Synchronous replication protects acknowledged writes while the follower is available. If the follower is unavailable, the primary does not acknowledge the write.
