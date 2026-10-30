"""End-to-end Phase 1 checks through the router.

The shard apps are mounted into the router's HTTP client, so the request path
is the real one -- plan, HTTP call per shard, merge -- without binding ports.
"""

import httpx
import pytest
from fastapi.testclient import TestClient

from shardstore.router import create_app as create_router
from shardstore.shard_node import create_app as create_shard
from shardstore.shardmap import ShardMap

SHARDS = [("shard-0", "127.0.0.1", 9101), ("shard-1", "127.0.0.1", 9102),
          ("shard-2", "127.0.0.1", 9103), ("shard-3", "127.0.0.1", 9104)]

PEOPLE = [
    {"_id": "u1", "name": "Ann", "city": "Delhi", "age": 30},
    {"_id": "u2", "name": "Bob", "city": "Pune", "age": 25},
    {"_id": "u3", "name": "Cat", "city": "Delhi", "age": 41},
    {"_id": "u4", "name": "Dev", "city": "Pune", "age": 33},
    {"_id": "u5", "name": "Eve", "city": "Goa", "age": 19},
    {"_id": "u6", "name": "Fay", "city": "Delhi", "age": 52},
]


@pytest.fixture()
def cluster(tmp_path):
    map_path = tmp_path / "shardmap.db"
    with ShardMap(map_path) as smap:
        smap.bootstrap(SHARDS, num_chunks=8)

    shard_apps = {
        f"all://{host}:{port}": create_shard(sid, tmp_path / f"{sid}.db")
        for sid, host, port in SHARDS
    }
    with TestClient(create_router(map_path)) as client:
        client.app.state.client = httpx.AsyncClient(
            mounts={url: httpx.ASGITransport(app=app) for url, app in shard_apps.items()}
        )
        yield client


@pytest.fixture()
def seeded(cluster):
    assert cluster.post("/documents/bulk", json={"documents": PEOPLE}).status_code == 201
    return cluster


def test_every_shard_is_reachable(cluster):
    health = cluster.get("/health").json()
    assert health["status"] == "online"
    assert health["shards_online"] == "4/4"


def test_bulk_insert_spreads_documents_over_several_shards(seeded):
    stats = seeded.get("/stats").json()
    assert stats["documents"] == len(PEOPLE)
    assert sum(1 for s in stats["shards"] if s["documents"]) > 1


def test_key_lookup_is_routed_directly(seeded):
    body = seeded.get("/documents/u3").json()
    assert body["document"] == PEOPLE[2]
    assert body["routing"]["mode"] == "direct"
    assert body["routing"]["fanout"] == 1


def test_every_document_is_retrievable_by_key(seeded):
    for doc in PEOPLE:
        assert seeded.get(f"/documents/{doc['_id']}").json()["document"] == doc


def test_insert_update_delete_round_trip(cluster):
    assert cluster.post("/documents", json={"document": PEOPLE[0]}).status_code == 201
    assert cluster.post("/documents", json={"document": PEOPLE[0]}).status_code == 409

    updated = cluster.put(
        "/documents/u1", json={"update": {"$inc": {"age": 1}, "$set": {"city": "Kochi"}}}
    ).json()
    assert updated["document"]["age"] == 31
    assert updated["routing"]["mode"] == "direct"
    assert cluster.get("/documents/u1").json()["document"]["city"] == "Kochi"

    assert cluster.delete("/documents/u1").json()["deleted"] == 1
    assert cluster.get("/documents/u1").status_code == 404
    assert cluster.delete("/documents/u1").status_code == 404


def test_filter_query_scatters_and_merges(seeded):
    body = seeded.post("/query", json={"filter": {"city": "Delhi"}, "sort": {"age": 1}}).json()
    assert body["routing"]["mode"] == "scatter-gather"
    assert body["routing"]["fanout"] == 4
    assert [d["_id"] for d in body["documents"]] == ["u1", "u3", "u6"]
    assert set(body["per_shard_ms"]) == {s[0] for s in SHARDS}


def test_scatter_gather_sorts_across_shard_boundaries(seeded):
    body = seeded.post("/query", json={"filter": {}, "sort": {"age": -1}}).json()
    ages = [d["age"] for d in body["documents"]]
    assert ages == sorted((p["age"] for p in PEOPLE), reverse=True)


def test_skip_and_limit_apply_to_the_merged_result(seeded):
    body = seeded.post(
        "/query", json={"filter": {}, "sort": {"age": 1}, "skip": 2, "limit": 2}
    ).json()
    assert [d["age"] for d in body["documents"]] == [30, 33]
    assert body["count"] == 2


def test_a_filter_pinning_the_shard_key_avoids_the_fan_out(seeded):
    body = seeded.post("/query", json={"filter": {"_id": "u4", "city": "Pune"}}).json()
    assert body["routing"]["mode"] == "direct"
    assert [d["_id"] for d in body["documents"]] == ["u4"]


def test_an_in_list_targets_only_the_owning_shards(seeded):
    body = seeded.post("/query", json={"filter": {"_id": {"$in": ["u1", "u2"]}}}).json()
    assert body["routing"]["mode"] == "multi-key"
    assert body["routing"]["fanout"] <= 2
    assert {d["_id"] for d in body["documents"]} == {"u1", "u2"}


def test_count_sums_the_shard_counts(seeded):
    body = seeded.post("/count", json={"filter": {"age": {"$gte": 30}}}).json()
    assert body["count"] == 4
    assert sum(body["per_shard"].values()) == 4


def test_projection_is_applied_on_the_shards(seeded):
    body = seeded.post("/query", json={"filter": {"city": "Delhi"}, "fields": ["name"]}).json()
    assert all(set(d) == {"_id", "name"} for d in body["documents"])


def test_explain_reports_the_plan_without_touching_the_shards(seeded):
    direct = seeded.post("/explain", json={"filter": {"_id": "u1"}}).json()
    scatter = seeded.post("/explain", json={"filter": {"city": "Delhi"}}).json()
    assert direct["routing"]["fanout"] == 1
    assert scatter["routing"]["fanout"] == 4


def test_bad_input_is_rejected_before_any_shard_is_called(cluster):
    assert cluster.post("/query", json={"filter": {"a": {"$nope": 1}}}).status_code == 400
    assert cluster.post("/documents", json={"document": {"name": "x"}}).status_code == 400


def test_routing_follows_the_shard_map_not_the_data(seeded):
    """Whatever the map says owns a key is the shard that answers for it."""
    snapshot = seeded.get("/shardmap").json()
    for doc in PEOPLE:
        body = seeded.get(f"/documents/{doc['_id']}").json()
        chunk = next(c for c in snapshot["chunks"] if c["chunk_id"] == body["routing"]["chunk_id"])
        assert body["routing"]["shards"] == [chunk["owner"]]
