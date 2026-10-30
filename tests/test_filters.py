"""The filter query language and its ordering."""

import pytest

from shardstore.filters import (
    FilterError, apply_sort, matches, normalise_sort, project, validate_filter,
)

DOCS = [
    {"_id": "a", "age": 30, "city": "Delhi", "tags": ["x", "y"], "p": {"q": 5}},
    {"_id": "b", "age": 25, "city": "Pune", "tags": []},
    {"_id": "c", "age": 41, "city": "Delhi"},
    {"_id": "d", "city": "Goa", "age": None},
]


def ids(filt):
    return [d["_id"] for d in DOCS if matches(d, filt)]


@pytest.mark.parametrize("filt,expected", [
    ({}, ["a", "b", "c", "d"]),
    (None, ["a", "b", "c", "d"]),
    ({"city": "Delhi"}, ["a", "c"]),
    ({"city": "Delhi", "age": {"$gte": 35}}, ["c"]),
    ({"age": {"$gt": 24, "$lt": 31}}, ["a", "b"]),
    ({"city": {"$in": ["Pune", "Goa"]}}, ["b", "d"]),
    ({"city": {"$nin": ["Pune", "Goa"]}}, ["a", "c"]),
    ({"p.q": 5}, ["a"]),
    ({"p": {"$exists": True}}, ["a"]),
    ({"tags": "x"}, ["a"]),
    ({"tags": {"$size": 0}}, ["b"]),
    ({"city": {"$regex": "^D"}}, ["a", "c"]),
    ({"$or": [{"city": "Goa"}, {"age": {"$gt": 40}}]}, ["c", "d"]),
    ({"$and": [{"city": "Delhi"}, {"tags": {"$exists": True}}]}, ["a"]),
    ({"$nor": [{"city": "Delhi"}]}, ["b", "d"]),
    ({"age": None}, ["d"]),
    ({"age": {"$type": "int"}}, ["a", "b", "c"]),
])
def test_matching(filt, expected):
    assert ids(filt) == expected


def test_missing_field_never_matches_a_comparison():
    assert matches({"_id": "z"}, {"age": {"$gt": 0}}) is False
    assert matches({"_id": "z"}, {"age": {"$exists": False}}) is True


def test_numbers_and_strings_do_not_compare():
    assert matches({"v": "10"}, {"v": {"$gt": 5}}) is False
    assert matches({"v": True}, {"v": 1}) is False


@pytest.mark.parametrize("bad", [
    {"a": {"$nope": 1}}, {"$where": "x"}, {"a": {"$in": 3}},
    {"a": {"$regex": "("}}, {"$and": []}, "not-a-dict",
])
def test_bad_filters_are_rejected(bad):
    with pytest.raises(FilterError):
        validate_filter(bad)


def test_sort_handles_missing_fields_and_both_directions():
    assert [d["_id"] for d in apply_sort(DOCS, normalise_sort({"age": 1}))] == ["d", "b", "a", "c"]
    assert [d["_id"] for d in apply_sort(DOCS, normalise_sort({"age": -1}))] == ["c", "a", "b", "d"]
    order = normalise_sort([["city", 1], ["age", -1]])
    assert [d["_id"] for d in apply_sort(DOCS, order)] == ["c", "a", "d", "b"]


def test_sort_spec_validation():
    assert normalise_sort({"a": 1}) == [("a", 1)]
    assert normalise_sort([["a", -1]]) == [("a", -1)]
    assert normalise_sort(None) == []
    with pytest.raises(FilterError):
        normalise_sort({"a": 2})


def test_projection_always_keeps_the_shard_key():
    assert project(DOCS[0], ["city"]) == {"_id": "a", "city": "Delhi"}
    assert project(DOCS[0], ["p.q"]) == {"_id": "a", "p.q": 5}
    assert project(DOCS[0], None) == DOCS[0]
