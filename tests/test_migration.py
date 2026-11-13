"""Chunk migration: capture, orphan filtering, cutover safety.

The in-process tests mount the shard apps into the router's HTTP client, so a
whole migration runs over the real request path without binding ports.
"""

import contextlib

import httpx
import pytest
from fastapi.testclient import TestClient

from shardstore.hashing import hash_key
from shardstore.migration import ChunkMigrator, MigrationError
from shardstore.router import create_app as create_router
from shardstore.shard_node import create_app as create_shard
from shardstore.shardmap import ShardMap
from shardstore.storage import DocumentStore, RangeNotOwned

SHARDS = [("shard-0", "127.0.0.1", 9101), ("shard-1", "127.0.0.1", 9102)]
HALF = 2 ** 31


# -- storage-level behaviour -------------------------------------------


@pytest.fixture()
def store(tmp_path):
    s = DocumentStore(tmp_path / "shard.db")
    yield s
    s.close()


def keys_in(lo, hi, count=300):
    return [f"u{i}" for i in range(count) if lo <= hash_key(f"u{i}") < hi]


def key_in_range(lo, hi, prefix="new"):
    """A fresh key that hashes into ``[lo, hi)``; suffixing an existing key
    would land it somewhere else entirely."""
    for i in range(100_000):
        candidate = f"{prefix}-{i}"
        if lo <= hash_key(candidate) < hi:
            return candidate
    raise AssertionError("no key found in range")


def test_arming_captures_only_mutations_inside_the_range(store):
    store.insert_many([{"_id": f"u{i}", "n": i} for i in range(300)])
    inside = keys_in(0, HALF)
    outside = keys_in(HALF, 2 ** 32)

    store.arm_range("m1", 0, HALF)
    store.update(inside[0], {"$set": {"n": 99}})
    store.delete(inside[1])
    store.update(outside[0], {"$set": {"n": 99}})

    captured = store.changes_since("m1", 0)
    assert [(c.op, c.doc_id) for c in captured] == [
        ("upsert", inside[0]), ("delete", inside[1])
    ]
    assert captured[0].document["n"] == 99
    assert captured[1].document is None


def test_the_armed_count_and_the_log_cannot_both_miss_a_write(store):
    """A write is either in the clone snapshot or in the changelog."""
    store.insert_many([{"_id": f"u{i}"} for i in range(300)])
    at_arm = store.arm_range("m1", 0, HALF)
    store.insert({"_id": key_in_range(0, HALF)})
    cloned = store.count(hash_range=(0, HALF))
    captured = len(store.changes_since("m1", 0))
    assert at_arm + 1 == cloned
    assert captured == 1


def test_changes_are_paged_and_report_the_backlog(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(300)])
    inside = keys_in(0, HALF)
    store.arm_range("m1", 0, HALF)
    for key in inside[:20]:
        store.update(key, {"$set": {"touched": True}})

    first = store.changes_since("m1", 0, limit=5)
    assert len(first) == 5
    assert store.pending_changes("m1", first[-1].seq) == 15
    rest = store.changes_since("m1", first[-1].seq, limit=100)
    assert len(rest) == 15
    assert store.pending_changes("m1", rest[-1].seq) == 0


def test_disarming_stops_capture_and_clears_the_log(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(300)])
    store.arm_range("m1", 0, HALF)
    store.update(keys_in(0, HALF)[0], {"$set": {"a": 1}})
    store.disarm_range("m1")
    assert store.changes_since("m1", 0) == []
    store.update(keys_in(0, HALF)[1], {"$set": {"a": 1}})
    assert store.changes_since("m1", 0) == []


def test_a_second_concurrent_donation_is_refused(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(50)])
    store.arm_range("m1", 0, HALF)
    with pytest.raises(Exception, match="already donating"):
        store.arm_range("m2", HALF, 2 ** 32)


def test_blocked_ranges_refuse_writes_but_not_reads(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(300)])
    inside = keys_in(0, HALF)
    store.set_blocked_ranges([(0, HALF)])

    assert store.get(inside[0]) is not None          # reads still served
    with pytest.raises(RangeNotOwned):
        store.update(inside[0], {"$set": {"a": 1}})
    with pytest.raises(RangeNotOwned):
        store.delete(inside[0])
    with pytest.raises(RangeNotOwned):
        store.insert({"_id": key_in_range(0, HALF)})

    outside = keys_in(HALF, 2 ** 32)
    store.update(outside[0], {"$set": {"a": 1}})     # other ranges unaffected

    store.set_blocked_ranges([])
    store.update(inside[0], {"$set": {"a": 1}})


def test_shipped_batches_are_idempotent(store):
    batch = [{"_id": "a", "n": 1}, {"_id": "b", "n": 2}]
    assert store.apply_migration_batch(batch, []) == (2, 0)
    assert store.apply_migration_batch(batch, []) == (2, 0)
    assert store.count() == 2                        # replay did not duplicate
    assert store.apply_migration_batch([], ["a", "missing"]) == (0, 1)
    assert store.apply_migration_batch([], ["a"]) == (0, 0)
    assert store.count() == 1


def test_owned_range_filtering_hides_orphans(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(300)])
    owned = [(0, HALF)]
    assert store.count(owned_ranges=owned) == len(keys_in(0, HALF))
    assert store.count(owned_ranges=[]) == 0
    assert len(store.query(owned_ranges=owned).documents) == len(keys_in(0, HALF))


def test_range_digest_matches_only_for_identical_contents(tmp_path):
    a, b = DocumentStore(tmp_path / "a.db"), DocumentStore(tmp_path / "b.db")
    docs = [{"_id": f"u{i}", "n": i} for i in range(100)]
    a.insert_many(docs)
    b.insert_many(docs)
    assert a.range_digest(0, HALF) == b.range_digest(0, HALF)
    b.update(keys_in(0, HALF, 100)[0], {"$set": {"n": -1}})
    assert a.range_digest(0, HALF) != b.range_digest(0, HALF)
    a.close()
    b.close()


# -- whole-cluster migration -------------------------------------------


@pytest.fixture()
def cluster(tmp_path):
    """Two shards and a router, wired together in-process."""
    map_path = tmp_path / "shardmap.db"
    with ShardMap(map_path) as smap:
        smap.bootstrap(SHARDS, num_chunks=2)

    apps = {sid: create_shard(sid, tmp_path / f"{sid}.db",
                              shard_map_path=str(map_path))
            for sid, _, _ in SHARDS}

    with contextlib.ExitStack() as stack:
        # Sync clients for the migrator (it drives shards synchronously) ...
        shard_clients = {
            f"http://{host}:{port}": stack.enter_context(TestClient(apps[sid]))
            for sid, host, port in SHARDS
        }
        # ... and async transports for the router's own client.
        transports = {f"all://{host}:{port}": httpx.ASGITransport(app=apps[sid])
                      for sid, host, port in SHARDS}
        client = stack.enter_context(TestClient(create_router(map_path)))
        client.app.state.client = httpx.AsyncClient(mounts=transports)
        client.shard_map_path = str(map_path)
        client.shard_clients = shard_clients
        yield client


class _MountedMigrator(ChunkMigrator):
    """A migrator that reaches the in-process shards instead of real sockets."""

    def __init__(self, path, shard_clients):
        super().__init__(path, verbose=False)
        self._shard_clients = shard_clients

    def _send(self, node, method, path, json_body, headers, params):
        return self._shard_clients[node.base_url].request(
            method, path, json=json_body, params=params, headers=headers)


@pytest.fixture()
def migrator(cluster):
    m = _MountedMigrator(cluster.shard_map_path, cluster.shard_clients)
    yield m
    m.close()


def seed(cluster, count=200):
    docs = [{"_id": f"u{i}", "n": i, "city": "Delhi" if i % 2 else "Pune"}
            for i in range(count)]
    assert cluster.post("/documents/bulk", json={"documents": docs}).status_code == 201
    return docs


def owner_of(cluster, chunk_id):
    chunks = cluster.get("/shardmap").json()["chunks"]
    return next(c["owner"] for c in chunks if c["chunk_id"] == chunk_id)


def test_migration_moves_the_range_and_flips_the_map(cluster, migrator):
    seed(cluster)
    before = cluster.post("/count", json={}).json()["count"]
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)

    report = migrator.migrate(0, destination)

    assert report.verified
    assert report.documents_moved > 0
    assert report.map_version_after > report.map_version_before
    assert owner_of(cluster, 0) == destination
    assert cluster.post("/count", json={}).json()["count"] == before


def test_no_document_is_lost_or_duplicated_by_the_move(cluster, migrator):
    docs = seed(cluster)
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)

    migrator.migrate(0, destination)

    found = cluster.post("/query", json={"filter": {}, "limit": 10_000}).json()
    ids = [d["_id"] for d in found["documents"]]
    assert len(ids) == len(set(ids))                 # no duplicates
    assert set(ids) == {d["_id"] for d in docs}      # nothing lost


def test_every_key_is_still_reachable_after_the_move(cluster, migrator):
    docs = seed(cluster, 60)
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)
    migrator.migrate(0, destination)

    for doc in docs:
        body = cluster.get(f"/documents/{doc['_id']}").json()
        assert body["document"]["n"] == doc["n"]
        assert body["routing"]["shards"] == [owner_of(cluster, body["routing"]["chunk_id"])]


def test_writes_accepted_during_the_copy_are_carried_over(cluster, migrator):
    """Writes landing after the clone must reach the recipient via catch-up.

    The writes are injected once cloning has finished, so the only way they can
    show up on the recipient is through the changelog.
    """
    seed(cluster)
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)
    chunk = next(c for c in cluster.get("/shardmap").json()["chunks"]
                 if c["chunk_id"] == 0)
    in_chunk = [f"u{i}" for i in range(200)
                if chunk["lo"] <= hash_key(f"u{i}") < chunk["hi"]]
    fresh = key_in_range(chunk["lo"], chunk["hi"], prefix="late")

    original_clone = migrator._clone

    def clone_then_write(*args, **kwargs):
        original_clone(*args, **kwargs)
        assert cluster.put(f"/documents/{in_chunk[0]}",
                           json={"update": {"$set": {"n": 9999}}}).status_code == 200
        assert cluster.delete(f"/documents/{in_chunk[1]}").status_code == 200
        assert cluster.post("/documents",
                            json={"document": {"_id": fresh, "n": 7}}).status_code == 201

    migrator._clone = clone_then_write
    report = migrator.migrate(0, destination)

    assert report.changes_applied >= 3
    assert report.deletes_applied >= 1
    assert cluster.get(f"/documents/{in_chunk[0]}").json()["document"]["n"] == 9999
    assert cluster.get(f"/documents/{in_chunk[1]}").status_code == 404
    assert cluster.get(f"/documents/{fresh}").json()["document"]["n"] == 7
    assert cluster.get(f"/documents/{fresh}").json()["routing"]["shards"] == [destination]


def test_a_chunk_can_be_moved_back(cluster, migrator):
    docs = seed(cluster, 60)
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)

    migrator.migrate(0, destination)
    assert owner_of(cluster, 0) == destination
    migrator.migrate(0, source)
    assert owner_of(cluster, 0) == source

    found = cluster.post("/query", json={"filter": {}, "limit": 10_000}).json()
    ids = [d["_id"] for d in found["documents"]]
    assert sorted(ids) == sorted(d["_id"] for d in docs)


def test_migrating_to_the_current_owner_is_refused(cluster, migrator):
    seed(cluster, 20)
    with pytest.raises(MigrationError, match="already owned"):
        migrator.migrate(0, owner_of(cluster, 0))


def test_unknown_chunk_and_shard_are_refused(cluster, migrator):
    seed(cluster, 20)
    with pytest.raises(MigrationError, match="no chunk"):
        migrator.migrate(99, "shard-1")
    with pytest.raises(MigrationError, match="unknown destination"):
        migrator.migrate(0, "shard-nope")


def test_an_aborted_migration_leaves_the_donor_in_charge(cluster, migrator):
    seed(cluster, 60)
    source = owner_of(cluster, 0)
    chunk = next(c for c in cluster.get("/shardmap").json()["chunks"]
                 if c["chunk_id"] == 0)
    donor = migrator.map.snapshot().shard(source)
    version = cluster.get("/shardmap").json()["version"]

    migrator._request(donor, "POST", "/internal/migration/arm",
                      {"migration_id": "doomed", "lo": chunk["lo"], "hi": chunk["hi"]})
    migrator._request(donor, "POST", "/internal/migration/freeze",
                      {"migration_id": "doomed"})
    migrator._request(donor, "POST", "/internal/migration/abort",
                      {"migration_id": "doomed"})

    assert owner_of(cluster, 0) == source
    assert cluster.get("/shardmap").json()["version"] == version
    in_chunk = next(f"u{i}" for i in range(60)
                    if chunk["lo"] <= hash_key(f"u{i}") < chunk["hi"])
    assert cluster.put(f"/documents/{in_chunk}",
                       json={"update": {"$set": {"n": 1}}}).status_code == 200


def test_a_frozen_range_blocks_writes_but_still_serves_reads(cluster, migrator):
    """The cutover critical section is write-only and is released on abort."""
    seed(cluster, 60)
    source = owner_of(cluster, 0)
    chunk = next(c for c in cluster.get("/shardmap").json()["chunks"]
                 if c["chunk_id"] == 0)
    other = next(c for c in cluster.get("/shardmap").json()["chunks"]
                 if c["chunk_id"] != 0)
    donor = migrator.map.snapshot().shard(source)
    in_chunk = next(f"u{i}" for i in range(60)
                    if chunk["lo"] <= hash_key(f"u{i}") < chunk["hi"])
    elsewhere = next(f"u{i}" for i in range(60)
                     if other["lo"] <= hash_key(f"u{i}") < other["hi"])

    migrator._request(donor, "POST", "/internal/migration/arm",
                      {"migration_id": "freeze-test", "lo": chunk["lo"], "hi": chunk["hi"]})
    migrator._request(donor, "POST", "/internal/migration/freeze",
                      {"migration_id": "freeze-test"})

    # Reads of the frozen range still succeed -- the donor is still the owner.
    assert cluster.get(f"/documents/{in_chunk}").status_code == 200
    # Writes are refused; the router retries, and with no cutover ever
    # completing it finally surfaces the conflict rather than hanging.
    blocked = cluster.put(f"/documents/{in_chunk}", json={"update": {"$set": {"n": 5}}})
    assert blocked.status_code == 409
    # A key outside the frozen range is unaffected.
    assert cluster.put(f"/documents/{elsewhere}",
                       json={"update": {"$set": {"n": 5}}}).status_code == 200

    migrator._request(donor, "POST", "/internal/migration/abort",
                      {"migration_id": "freeze-test"})
    assert cluster.put(f"/documents/{in_chunk}",
                       json={"update": {"$set": {"n": 5}}}).status_code == 200


def test_the_donor_sheds_the_range_and_the_recipient_holds_it(cluster, migrator):
    seed(cluster)
    source = owner_of(cluster, 0)
    destination = next(s for s, _, _ in SHARDS if s != source)
    report = migrator.migrate(0, destination)

    assert report.verification["source_documents_left"] == 0
    assert report.verification["destination_documents"] == report.documents_moved
    assert report.removed_from_source == report.verification["destination_documents"]
