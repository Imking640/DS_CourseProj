"""Shard map: chunk coverage, key lookup, versioning."""

from collections import Counter

import pytest

from shardstore.hashing import HASH_SPACE, even_ranges, hash_key
from shardstore.shardmap import ShardMap, ShardMapCache, ShardMapError

SHARDS = [("shard-0", "127.0.0.1", 9101), ("shard-1", "127.0.0.1", 9102),
          ("shard-2", "127.0.0.1", 9103), ("shard-3", "127.0.0.1", 9104)]


@pytest.fixture()
def smap(tmp_path):
    with ShardMap(tmp_path / "shardmap.db") as m:
        m.bootstrap(SHARDS, num_chunks=8)
        yield m


def test_hash_is_stable_and_bounded():
    assert hash_key("user-1") == hash_key("user-1")
    assert hash_key(7) != hash_key("7")
    assert all(0 <= hash_key(f"k{i}") < HASH_SPACE for i in range(500))


def test_even_ranges_tile_the_whole_space():
    for n in (1, 3, 8, 17):
        ranges = even_ranges(n)
        assert ranges[0][0] == 0 and ranges[-1][1] == HASH_SPACE
        assert all(a[1] == b[0] for a, b in zip(ranges, ranges[1:]))


def test_bootstrap_covers_the_space_evenly(smap):
    snap = smap.snapshot()
    assert len(snap.chunks) == 8
    assert sum(c.hi - c.lo for c in snap.chunks) == HASH_SPACE
    assert set(snap.coverage().values()) == {0.25}


def test_lookup_is_deterministic_and_spread(smap):
    snap = smap.snapshot()
    keys = [f"user-{i}" for i in range(4000)]
    first = [snap.shard_for_key(k).shard_id for k in keys]
    assert first == [snap.shard_for_key(k).shard_id for k in keys]
    counts = Counter(first)
    assert len(counts) == 4
    assert all(800 < n < 1200 for n in counts.values())


def test_key_always_lands_in_its_chunk(smap):
    snap = smap.snapshot()
    for i in range(500):
        key = f"user-{i}"
        assert snap.chunk_for_key(key).contains(hash_key(key))


def test_reassigning_a_chunk_bumps_the_version(smap):
    before = smap.version
    smap.set_chunk_owner(3, "shard-0")
    assert smap.version == before + 1
    assert smap.snapshot().chunks[3].owner == "shard-0"


def test_cache_reloads_only_when_the_version_moves(tmp_path):
    path = tmp_path / "shardmap.db"
    with ShardMap(path) as m:
        m.bootstrap(SHARDS, num_chunks=8)
        cache = ShardMapCache(path)
        version = cache.snapshot.version
        cache.snapshot, cache.snapshot
        assert cache.reloads == 0
        m.set_chunk_owner(2, "shard-1")
        assert cache.snapshot.version == version + 1
        assert cache.reloads == 1
        cache.close()


def test_bootstrap_refuses_to_clobber_an_existing_map(smap):
    with pytest.raises(ShardMapError):
        smap.bootstrap(SHARDS, num_chunks=8)


def test_snapshot_rejects_a_gap_in_the_chunk_table(smap):
    smap._conn.execute("DELETE FROM chunks WHERE chunk_id = 4")
    smap._conn.commit()
    with pytest.raises(ShardMapError):
        smap.snapshot()
