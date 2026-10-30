"""Synthetic documents and latency statistics used by the benchmark scripts."""

from __future__ import annotations

import random
from typing import Any, Sequence

CITIES = ["Delhi", "Mumbai", "Pune", "Hyderabad", "Chennai", "Kolkata", "Goa", "Kochi"]
PLANS = ["free", "basic", "pro", "enterprise"]
TAGS = ["alpha", "beta", "gamma", "delta", "epsilon"]


def make_document(index: int, rng: random.Random, prefix: str = "user") -> dict[str, Any]:
    return {
        "_id": f"{prefix}-{index}",
        "name": f"user_{index}",
        "city": rng.choice(CITIES),
        "plan": rng.choice(PLANS),
        "age": rng.randint(18, 70),
        "score": round(rng.uniform(0, 100), 2),
        "active": rng.random() < 0.7,
        "tags": rng.sample(TAGS, k=rng.randint(1, 3)),
        "profile": {"country": "IN", "signup_year": rng.randint(2015, 2025)},
    }


def make_documents(count: int, seed: int = 42, prefix: str = "user") -> list[dict[str, Any]]:
    rng = random.Random(seed)
    return [make_document(i, rng, prefix) for i in range(count)]


def percentile(samples: Sequence[float], q: float) -> float:
    """Nearest-rank percentile of an unsorted sample list."""
    if not samples:
        return 0.0
    ordered = sorted(samples)
    rank = max(0, min(len(ordered) - 1, round(q / 100 * len(ordered) + 0.5) - 1))
    return ordered[rank]


def summarise(name: str, samples: Sequence[float], wall_s: float, errors: int = 0) -> dict[str, Any]:
    """Latency summary in milliseconds, plus achieved throughput."""
    return {
        "workload": name,
        "n": len(samples),
        "errors": errors,
        "mean_ms": round(sum(samples) / len(samples), 3) if samples else 0.0,
        "p50_ms": round(percentile(samples, 50), 3),
        "p95_ms": round(percentile(samples, 95), 3),
        "p99_ms": round(percentile(samples, 99), 3),
        "max_ms": round(max(samples), 3) if samples else 0.0,
        "ops_per_s": round(len(samples) / wall_s, 1) if wall_s > 0 else 0.0,
    }
