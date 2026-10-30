"""A small MongoDB-flavoured filter and sort API.

A filter is a JSON object mapping dotted field paths to either a literal
(equality) or an operator document, e.g.::

    {"city": "Delhi", "age": {"$gte": 30}, "tags": {"$in": ["a", "b"]}}

Clauses are ANDed.  ``$and`` / ``$or`` / ``$not`` take nested filters.  The
same module provides the ordering used both inside a shard and by the router
when it merges scatter-gather results, so a sorted merge is exactly a sorted
scan of the union.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Iterable, Sequence

from .hashing import canonical

MISSING = object()

COMPARISON_OPS = {"$eq", "$ne", "$gt", "$gte", "$lt", "$lte"}
VALUE_OPS = COMPARISON_OPS | {"$in", "$nin", "$exists", "$regex", "$size", "$type"}
LOGICAL_OPS = {"$and", "$or", "$not", "$nor"}


class FilterError(ValueError):
    """Raised for a malformed filter or sort specification."""


# -- ordering ----------------------------------------------------------
#
# Values of different JSON types are still totally ordered (as in BSON) so a
# sort never raises on heterogeneous or missing fields.
_TYPE_RANK = {
    type(MISSING): 0,
    type(None): 1,
    bool: 2,
    int: 3,
    float: 3,
    str: 4,
    list: 5,
    dict: 6,
}


def type_name(value: Any) -> str:
    if value is None:
        return "null"
    return {bool: "bool", int: "int", float: "double", str: "string",
            list: "array", dict: "object"}.get(type(value), type(value).__name__)


def sort_key(value: Any) -> tuple[int, Any]:
    """Total order across JSON types; ``MISSING`` sorts before ``null``."""
    if value is MISSING:
        return (0, 0)
    rank = _TYPE_RANK.get(type(value), 7)
    if rank == 3:
        return (3, float(value))
    if rank in (2, 4):
        return (rank, value)
    if rank == 1:
        return (1, 0)
    if rank == 5:
        return (5, tuple(sort_key(v) for v in value))
    if rank == 6:
        return (6, tuple((k, sort_key(v)) for k, v in sorted(value.items())))
    return (7, str(value))


def get_path(doc: Any, path: str) -> Any:
    """Resolve a dotted path, returning ``MISSING`` if any step is absent."""
    current = doc
    for part in path.split("."):
        if isinstance(current, dict):
            if part not in current:
                return MISSING
            current = current[part]
        elif isinstance(current, list) and part.isdigit():
            index = int(part)
            if index >= len(current):
                return MISSING
            current = current[index]
        else:
            return MISSING
    return current


def _comparable(a: Any, b: Any) -> bool:
    """True when ``a`` and ``b`` sit in the same comparison class."""
    if isinstance(a, bool) != isinstance(b, bool):
        return False
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return True
    return type(a) is type(b)


def _equals(actual: Any, expected: Any) -> bool:
    if actual is MISSING:
        return expected is None
    if isinstance(actual, list) and not isinstance(expected, list):
        return any(_equals(item, expected) for item in actual)
    if isinstance(actual, bool) != isinstance(expected, bool):
        return False
    return actual == expected


def _ordered(actual: Any, expected: Any, op: str) -> bool:
    if actual is MISSING or not _comparable(actual, expected):
        return False
    a, b = sort_key(actual), sort_key(expected)
    return {"$gt": a > b, "$gte": a >= b, "$lt": a < b, "$lte": a <= b}[op]


def _match_operator(op: str, actual: Any, operand: Any) -> bool:
    if op == "$eq":
        return _equals(actual, operand)
    if op == "$ne":
        return not _equals(actual, operand)
    if op in ("$gt", "$gte", "$lt", "$lte"):
        if isinstance(actual, list):
            return any(_ordered(item, operand, op) for item in actual)
        return _ordered(actual, operand, op)
    if op == "$in":
        return any(_equals(actual, v) for v in operand)
    if op == "$nin":
        return not any(_equals(actual, v) for v in operand)
    if op == "$exists":
        return (actual is not MISSING) == bool(operand)
    if op == "$regex":
        return isinstance(actual, str) and re.search(operand, actual) is not None
    if op == "$size":
        return isinstance(actual, list) and len(actual) == operand
    if op == "$type":
        return actual is not MISSING and type_name(actual) == operand
    raise FilterError(f"unsupported operator {op!r}")


def matches(doc: dict[str, Any], filt: dict[str, Any] | None) -> bool:
    """Does ``doc`` satisfy ``filt``?  An empty filter matches everything."""
    if not filt:
        return True
    for field, condition in filt.items():
        if field == "$and":
            if not all(matches(doc, sub) for sub in condition):
                return False
        elif field == "$or":
            if not any(matches(doc, sub) for sub in condition):
                return False
        elif field == "$nor":
            if any(matches(doc, sub) for sub in condition):
                return False
        elif field == "$not":
            if matches(doc, condition):
                return False
        elif isinstance(condition, dict) and any(k.startswith("$") for k in condition):
            actual = get_path(doc, field)
            for op, operand in condition.items():
                if not _match_operator(op, actual, operand):
                    return False
        else:
            if not _equals(get_path(doc, field), condition):
                return False
    return True


def validate_filter(filt: Any, _depth: int = 0) -> None:
    """Reject malformed filters up front so shards fail identically."""
    if filt is None:
        return
    if not isinstance(filt, dict):
        raise FilterError("filter must be a JSON object")
    if _depth > 8:
        raise FilterError("filter nested too deeply")
    for field, condition in filt.items():
        if field in ("$and", "$or", "$nor"):
            if not isinstance(condition, list) or not condition:
                raise FilterError(f"{field} expects a non-empty list of filters")
            for sub in condition:
                validate_filter(sub, _depth + 1)
        elif field == "$not":
            validate_filter(condition, _depth + 1)
        elif field.startswith("$"):
            raise FilterError(f"unsupported top-level operator {field!r}")
        elif isinstance(condition, dict) and any(k.startswith("$") for k in condition):
            for op, operand in condition.items():
                if op not in VALUE_OPS:
                    raise FilterError(f"unsupported operator {op!r} on field {field!r}")
                if op in ("$in", "$nin") and not isinstance(operand, list):
                    raise FilterError(f"{op} expects a list")
                if op == "$regex":
                    try:
                        re.compile(operand)
                    except (re.error, TypeError) as exc:
                        raise FilterError(f"invalid $regex: {exc}") from exc


# -- sort / projection -------------------------------------------------

Sort = list[tuple[str, int]]


def normalise_sort(spec: Any) -> Sort:
    """Accept ``{"age": -1}`` or ``[["age", -1], ...]`` and normalise it."""
    if not spec:
        return []
    pairs: Iterable[Sequence[Any]]
    pairs = spec.items() if isinstance(spec, dict) else spec
    out: Sort = []
    for pair in pairs:
        if isinstance(pair, str):
            field, direction = pair, 1
        elif len(pair) == 2:
            field, direction = pair
        else:
            raise FilterError(f"bad sort entry {pair!r}")
        if direction not in (1, -1):
            raise FilterError("sort direction must be 1 or -1")
        out.append((str(field), int(direction)))
    return out


def sort_comparator(sort: Sort) -> Callable[[dict[str, Any]], tuple[Any, ...]]:
    """Build a ``key=`` function; descending fields are rank-negated."""

    def key(doc: dict[str, Any]) -> tuple[Any, ...]:
        parts: list[Any] = []
        for field, direction in sort:
            rank, value = sort_key(get_path(doc, field))
            parts.append((-rank, _Reversed(value)) if direction < 0 else (rank, value))
        return tuple(parts)

    return key


class _Reversed:
    """Wrapper that inverts comparison, for descending sort on mixed types."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __lt__(self, other: "_Reversed") -> bool:
        return other.value < self.value

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Reversed) and other.value == self.value


def apply_sort(docs: list[dict[str, Any]], sort: Sort) -> list[dict[str, Any]]:
    return sorted(docs, key=sort_comparator(sort)) if sort else docs


def project(doc: dict[str, Any], fields: Sequence[str] | None) -> dict[str, Any]:
    """Keep only the named fields (``_id`` is always kept)."""
    if not fields:
        return doc
    out = {"_id": doc.get("_id")} if "_id" in doc else {}
    for field in fields:
        value = get_path(doc, field)
        if value is not MISSING:
            out[field] = value
    return out


# -- shard-key analysis ------------------------------------------------


class KeySet:
    """A set of shard-key values, deduplicated by canonical JSON form.

    ``None`` is used elsewhere to mean "unconstrained"; this type only ever
    represents an explicit, finite set of candidate keys.
    """

    __slots__ = ("_values",)

    def __init__(self, values: Iterable[Any] = ()) -> None:
        self._values: dict[str, Any] = {canonical(v): v for v in values}

    def __iter__(self):
        return iter(self._values.values())

    def __len__(self) -> int:
        return len(self._values)

    def __and__(self, other: "KeySet") -> "KeySet":
        out = KeySet()
        out._values = {k: v for k, v in self._values.items() if k in other._values}
        return out

    def __or__(self, other: "KeySet") -> "KeySet":
        out = KeySet()
        out._values = {**self._values, **other._values}
        return out

    def __repr__(self) -> str:
        return f"KeySet({list(self._values.values())!r})"


def _narrow(current: KeySet | None, found: KeySet | None) -> KeySet | None:
    """Intersect two constraints, where ``None`` means "no constraint"."""
    if found is None:
        return current
    return found if current is None else current & found


def extract_shard_keys(filt: Any, shard_key: str = "_id") -> KeySet | None:
    """Pull the shard-key equality constraints out of a filter.

    Returns ``None`` when the filter does not pin the shard key.  The router
    reads this to choose between direct routing and scatter-gather, and a
    shard reads it to turn a pinned filter into a primary-key lookup instead
    of a collection scan.
    """
    if not isinstance(filt, dict) or not filt:
        return None
    keys: KeySet | None = None
    for field_name, condition in filt.items():
        if field_name == "$and":
            for sub in condition:
                keys = _narrow(keys, extract_shard_keys(sub, shard_key))
        elif field_name == "$or":
            branch: KeySet | None = None
            for sub in condition:
                sub_keys = extract_shard_keys(sub, shard_key)
                if sub_keys is None:          # one unconstrained branch ...
                    branch = None             # ... makes the whole $or unconstrained
                    break
                branch = sub_keys if branch is None else branch | sub_keys
            keys = _narrow(keys, branch)
        elif field_name == shard_key:
            if isinstance(condition, dict) and any(k.startswith("$") for k in condition):
                if "$eq" in condition:
                    keys = _narrow(keys, KeySet([condition["$eq"]]))
                elif "$in" in condition and isinstance(condition["$in"], list):
                    keys = _narrow(keys, KeySet(condition["$in"]))
            else:
                keys = _narrow(keys, KeySet([condition]))
    return keys
