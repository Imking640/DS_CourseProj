"""Route planning and result merging, independent of HTTP."""

import pytest

from shardstore.filters import normalise_sort
from shardstore.routing import (
    DIRECT, MULTI_KEY, SCATTER_GATHER, extract_shard_keys, merge_results,
    plan_for_filter, plan_for_key, shard_limit,
)
from shardstore.shardmap import ShardMap

SHARDS = [("shard-0", "127.0.0.1", 9101), ("shard-1", "127.0.0.1", 9102),
          ("shard-2", "127.0.0.1", 9103), ("shard-3", "127.0.0.1", 9104)]


@pytest.fixture()
def snap(tmp_path):
    with ShardMap(tmp_path / "shardmap.db") as m:
        yield m.bootstrap(SHARDS, num_chunks=8)


@pytest.mark.parametrize("filt,expected", [
    ({"_id": "u1"}, ["u1"]),
    ({"_id": {"$eq": "u1"}}, ["u1"]),
    ({"_id": {"$in": ["u1", "u2"]}}, ["u1", "u2"]),
    ({"_id": "u1", "city": "Delhi"}, ["u1"]),
    ({"$and": [{"_id": "u1"}, {"age": {"$gt": 3}}]}, ["u1"]),
    ({"$or": [{"_id": "u1"}, {"_id": "u2"}]}, ["u1", "u2"]),
    ({"$and": [{"_id": {"$in": ["u1", "u2"]}}, {"_id": "u2"}]}, ["u2"]),
])
def test_filters_that_pin_the_shard_key(filt, expected, snap):
    assert sorted(extract_shard_keys(filt, "_id")) == sorted(expected)


@pytest.mark.parametrize("filt", [
    {}, None, {"city": "Delhi"},
    {"_id": {"$gt": "u1"}},
    {"_id": {"$ne": "u1"}},
    {"$or": [{"_id": "u1"}, {"city": "Delhi"}]},
])
def test_filters_that_do_not_pin_the_shard_key(filt, snap):
    assert extract_shard_keys(filt, "_id") is None


def test_key_lookup_targets_one_shard(snap):
    plan = plan_for_key(snap, "u1")
    assert plan.mode == DIRECT and plan.fanout == 1
    assert plan.chunk.contains(snap.chunk_for_key("u1").lo)
    assert plan.targets[0].shard_id == snap.chunk_for_key("u1").owner


def test_plan_modes(snap):
    assert plan_for_filter(snap, {"_id": "u1"}).mode == DIRECT
    assert plan_for_filter(snap, {"city": "Delhi"}).mode == SCATTER_GATHER
    assert plan_for_filter(snap, {"city": "Delhi"}).fanout == 4
    multi = plan_for_filter(snap, {"_id": {"$in": [f"u{i}" for i in range(40)]}})
    assert multi.mode == MULTI_KEY and multi.fanout == 4


def test_multi_key_plan_lists_each_shard_once(snap):
    plan = plan_for_filter(snap, {"_id": {"$in": [f"u{i}" for i in range(60)]}})
    assert len(plan.targets) == len({s.shard_id for s in plan.targets})


def test_plan_reports_the_map_version(snap):
    assert plan_for_filter(snap, {"city": "X"}).as_info()["map_version"] == snap.version


def test_per_shard_limit_covers_the_global_window():
    assert shard_limit(5, 0, 1) == 5        # single shard: no inflation
    assert shard_limit(5, 10, 1) == 15      # skip still needs the prefix
    assert shard_limit(5, 10, 4) == 15      # every shard could own the window
    assert shard_limit(None, 0, 4) is None


def test_sorted_merge_is_globally_ordered():
    per_shard = [[{"n": 1}, {"n": 5}, {"n": 9}], [{"n": 2}, {"n": 6}], [{"n": 3}]]
    merged = merge_results(per_shard, normalise_sort({"n": 1}))
    assert [d["n"] for d in merged] == [1, 2, 3, 5, 6, 9]


def test_merge_applies_skip_then_limit():
    per_shard = [[{"n": 1}, {"n": 5}], [{"n": 2}, {"n": 6}]]
    merged = merge_results(per_shard, normalise_sort({"n": 1}), limit=2, skip=1)
    assert [d["n"] for d in merged] == [2, 5]


def test_unsorted_merge_concatenates_every_shard():
    per_shard = [[{"n": 1}], [{"n": 2}], [], [{"n": 3}]]
    assert len(merge_results(per_shard, [])) == 3
