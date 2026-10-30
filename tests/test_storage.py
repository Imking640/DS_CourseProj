"""Per-shard document storage: CRUD, update operators, range scans."""

import pytest

from shardstore.filters import FilterError
from shardstore.hashing import hash_key
from shardstore.storage import (
    DocumentNotFound, DocumentStore, DuplicateKeyError, apply_update,
)


@pytest.fixture()
def store(tmp_path):
    s = DocumentStore(tmp_path / "shard.db")
    yield s
    s.close()


def test_insert_get_delete_roundtrip(store):
    store.insert({"_id": "u1", "age": 30})
    assert store.get("u1") == {"_id": "u1", "age": 30}
    assert store.delete("u1") is True
    assert store.get("u1") is None
    assert store.delete("u1") is False


def test_duplicate_insert_is_rejected(store):
    store.insert({"_id": "u1"})
    with pytest.raises(DuplicateKeyError):
        store.insert({"_id": "u1", "age": 1})
    assert store.get("u1") == {"_id": "u1"}


def test_insert_without_the_shard_key_is_rejected(store):
    with pytest.raises(FilterError):
        store.insert({"age": 1})


def test_bulk_insert_is_all_or_nothing(store):
    store.insert({"_id": "u1"})
    with pytest.raises(DuplicateKeyError):
        store.insert_many([{"_id": "u2"}, {"_id": "u1"}])
    assert store.count() == 1
    assert store.insert_many([{"_id": "u2"}, {"_id": "u3"}]) == 2
    assert store.count() == 3


def test_update_operators(store):
    store.insert({"_id": "u1", "age": 30, "tags": ["x"]})
    doc, created = store.update(
        "u1", {"$set": {"a.b": 1}, "$inc": {"age": 2}, "$push": {"tags": "y"}}
    )
    assert created is False
    assert doc == {"_id": "u1", "age": 32, "tags": ["x", "y"], "a": {"b": 1}}
    doc, _ = store.update("u1", {"$unset": {"a.b": ""}, "$pull": {"tags": "x"}})
    assert doc["a"] == {} and doc["tags"] == ["y"]


def test_update_without_operators_replaces_but_keeps_the_id(store):
    store.insert({"_id": "u1", "age": 30, "city": "Delhi"})
    doc, _ = store.update("u1", {"age": 1})
    assert doc == {"age": 1, "_id": "u1"}


def test_update_bumps_the_document_version(store):
    store.insert({"_id": "u1", "n": 0})
    for _ in range(3):
        store.update("u1", {"$inc": {"n": 1}})
    row = store._conn.execute("SELECT version FROM documents WHERE doc_id='u1'").fetchone()
    assert row["version"] == 4
    assert store.get("u1")["n"] == 3


def test_update_requires_an_existing_document_unless_upserting(store):
    with pytest.raises(DocumentNotFound):
        store.update("u1", {"$set": {"a": 1}})
    doc, created = store.update("u1", {"$set": {"a": 1}}, upsert=True)
    assert created is True and doc == {"_id": "u1", "a": 1}


def test_the_shard_key_is_immutable(store):
    store.insert({"_id": "u1"})
    with pytest.raises(FilterError):
        store.update("u1", {"$set": {"_id": "u2"}})


def test_mixing_operators_and_plain_fields_is_rejected():
    with pytest.raises(FilterError):
        apply_update({"_id": "u1"}, {"$set": {"a": 1}, "b": 2})


def test_find_applies_filter_sort_skip_and_limit(store):
    store.insert_many([{"_id": f"u{i}", "age": i, "even": i % 2 == 0} for i in range(10)])
    assert [d["_id"] for d in store.find({"even": True}, sort={"age": -1}, limit=2)] == ["u8", "u6"]
    assert [d["_id"] for d in store.find({}, sort={"age": 1}, skip=8)] == ["u8", "u9"]
    assert store.find({"age": {"$gte": 5}}, fields=["age"])[0].keys() == {"_id", "age"}


def test_range_scan_matches_the_stored_key_hash(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(200)])
    lo, hi = 0, 2 ** 31
    expected = {f"u{i}" for i in range(200) if lo <= hash_key(f"u{i}") < hi}
    assert {d["_id"] for d in store.find(hash_range=(lo, hi))} == expected
    assert store.count(hash_range=(lo, hi)) == len(expected)


def test_delete_range_removes_only_that_range(store):
    store.insert_many([{"_id": f"u{i}"} for i in range(200)])
    lo, hi = 0, 2 ** 31
    removed = store.delete_range(lo, hi)
    assert removed == sum(1 for i in range(200) if lo <= hash_key(f"u{i}") < hi)
    assert store.count() == 200 - removed
    assert store.count(hash_range=(lo, hi)) == 0


def test_data_survives_reopening_the_file(tmp_path):
    path = tmp_path / "shard.db"
    first = DocumentStore(path)
    first.insert({"_id": "u1", "age": 7})
    first.close()
    second = DocumentStore(path)
    assert second.get("u1") == {"_id": "u1", "age": 7}
    second.close()
