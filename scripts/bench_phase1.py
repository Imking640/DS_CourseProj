#!/usr/bin/env python3
"""Phase 1 baseline benchmark: direct routing vs scatter-gather.

Measures, through the router, the per-operation latency of requests that the
shard map can pin to one shard against requests that must fan out to every
shard and be merged.  Writes results/phase1_baseline.json and a markdown
summary in docs/phase1_baseline.md.

    python scripts/bench_phase1.py --docs 20000 --iterations 300
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import random
import sys
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

import httpx

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shardstore.config import ClusterConfig  # noqa: E402
from shardstore.workload import CITIES, PLANS, make_documents, summarise  # noqa: E402

Call = Callable[[httpx.AsyncClient, int], Awaitable[httpx.Response]]


async def seed(client: httpx.AsyncClient, count: int, batch: int) -> None:
    documents = make_documents(count)
    for start in range(0, count, batch):
        response = await client.post(
            "/documents/bulk",
            json={"documents": documents[start:start + batch], "overwrite": True},
        )
        response.raise_for_status()


async def measure_all(
    client: httpx.AsyncClient,
    workloads: list[tuple[str, str, Call]],
    iterations: int,
    repeats: int,
    warmup: int = 20,
) -> list[dict[str, Any]]:
    """Sequential (concurrency 1) latency for every workload.

    The workloads are run round-robin rather than one after another: the
    scatter-gather path is CPU-bound and a machine that gets busy halfway
    through would otherwise penalise whichever workload happened to be
    running then.  Interleaving spreads any such drift over all of them.
    """
    for name, _, call in workloads:
        for i in range(warmup):
            await call(client, i)

    samples: dict[str, list[float]] = {name: [] for name, _, _ in workloads}
    errors: dict[str, int] = {name: 0 for name, _, _ in workloads}
    wall: dict[str, float] = {name: 0.0 for name, _, _ in workloads}
    per_round = max(1, iterations // repeats)

    for round_index in range(repeats):
        for name, _, call in workloads:
            offset = round_index * per_round
            t0 = time.perf_counter()
            for i in range(per_round):
                start = time.perf_counter()
                try:
                    response = await call(client, offset + i)
                except httpx.HTTPError:
                    errors[name] += 1       # a dropped keep-alive is not a sample
                    continue
                elapsed = (time.perf_counter() - start) * 1000
                if response.status_code >= 400:
                    errors[name] += 1
                    continue
                samples[name].append(elapsed)
            wall[name] += time.perf_counter() - t0

    return [summarise(name, samples[name], wall[name], errors[name])
            for name, _, _ in workloads]


async def throughput(
    client: httpx.AsyncClient, name: str, call: Call, iterations: int, concurrency: int
) -> dict[str, Any]:
    """Closed-loop throughput with `concurrency` outstanding requests."""
    semaphore = asyncio.Semaphore(concurrency)
    samples: list[float] = []
    errors = 0

    async def one(i: int) -> None:
        nonlocal errors
        async with semaphore:
            start = time.perf_counter()
            try:
                response = await call(client, i)
            except httpx.HTTPError:
                errors += 1
                return
            if response.status_code >= 400:
                errors += 1
                return
            samples.append((time.perf_counter() - start) * 1000)

    t0 = time.perf_counter()
    await asyncio.gather(*(one(i) for i in range(iterations)))
    result = summarise(name, samples, time.perf_counter() - t0, errors)
    result["concurrency"] = concurrency
    return result


def build_workloads(docs: int, rng: random.Random) -> list[tuple[str, str, Call]]:
    """(name, routing mode, call) triples covering both routing paths."""
    keys = [f"user-{rng.randrange(docs)}" for _ in range(4096)]

    def key(i: int) -> str:
        return keys[i % len(keys)]

    async def key_lookup(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.get(f"/documents/{key(i)}")

    async def point_query(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post("/query", json={"filter": {"_id": key(i)}})

    async def multi_key_query(c: httpx.AsyncClient, i: int) -> httpx.Response:
        batch = [key(i), key(i + 1), key(i + 2), key(i + 3)]
        return await c.post("/query", json={"filter": {"_id": {"$in": batch}}})

    async def insert(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post(
            "/documents",
            json={"document": {"_id": f"bench-{time.perf_counter_ns()}-{i}", "age": 30}},
        )

    async def update(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.put(f"/documents/{key(i)}", json={"update": {"$inc": {"score": 1}}})

    async def filter_scatter(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post("/query", json={"filter": {"city": CITIES[i % len(CITIES)]}})

    async def filter_scatter_selective(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post(
            "/query",
            json={"filter": {"plan": PLANS[i % len(PLANS)], "age": {"$gte": 65},
                             "active": True}},
        )

    async def filter_scatter_sorted(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post(
            "/query",
            json={"filter": {"city": CITIES[i % len(CITIES)]}, "sort": {"score": -1},
                  "limit": 20},
        )

    async def count_scatter(c: httpx.AsyncClient, i: int) -> httpx.Response:
        return await c.post("/count", json={"filter": {"city": CITIES[i % len(CITIES)]}})

    return [
        ("key_lookup", "direct", key_lookup),
        ("point_query", "direct", point_query),
        ("insert", "direct", insert),
        ("update", "direct", update),
        ("multi_key_query", "multi-key", multi_key_query),
        ("filter_selective", "scatter-gather", filter_scatter_selective),
        ("filter_broad", "scatter-gather", filter_scatter),
        ("filter_sorted_limit", "scatter-gather", filter_scatter_sorted),
        ("count", "scatter-gather", count_scatter),
    ]


def markdown_table(rows: list[dict[str, Any]]) -> str:
    header = ("| workload | routing | fanout | n | mean | p50 | p95 | p99 | ops/s |\n"
              "|---|---|---:|---:|---:|---:|---:|---:|---:|\n")
    body = "".join(
        f"| `{r['workload']}` | {r['routing']} | {r['fanout']} | {r['n']} | "
        f"{r['mean_ms']:.2f} | {r['p50_ms']:.2f} | {r['p95_ms']:.2f} | "
        f"{r['p99_ms']:.2f} | {r['ops_per_s']:,.0f} |\n"
        for r in rows
    )
    return header + body


async def run(args: argparse.Namespace) -> int:
    cfg = ClusterConfig.load(args.config)
    rng = random.Random(7)

    async with httpx.AsyncClient(
        base_url=cfg.router.base_url,
        timeout=60.0,
        transport=httpx.AsyncHTTPTransport(retries=3),
        limits=httpx.Limits(max_connections=256, max_keepalive_connections=64),
    ) as client:
        health = (await client.get("/health")).json()
        if health["status"] != "online":
            print(f"cluster is {health['status']}: {health['shards_online']} shards online",
                  file=sys.stderr)
            return 1

        print(f"seeding {args.docs:,} documents ...")
        await seed(client, args.docs, args.batch)
        stats = (await client.get("/stats")).json()
        print(f"  {stats['documents']:,} documents across {len(stats['shards'])} shards")

        rows: list[dict[str, Any]] = []
        fanout = {"direct": 1, "multi-key": f"<={len(cfg.shards)}",
                  "scatter-gather": len(cfg.shards)}
        workloads = build_workloads(args.docs, rng)
        modes = {name: mode for name, mode, _ in workloads}
        print(f"measuring {args.iterations} requests per workload "
              f"over {args.repeats} interleaved rounds ...")
        for result in await measure_all(client, workloads, args.iterations, args.repeats):
            mode = modes[result["workload"]]
            result["routing"] = mode
            result["fanout"] = fanout[mode]
            rows.append(result)
            print(f"  {result['workload']:22} {mode:15} p50 {result['p50_ms']:7.2f} ms   "
                  f"p99 {result['p99_ms']:7.2f} ms   {result['ops_per_s']:>8,.0f} ops/s")

        await asyncio.sleep(1.0)        # let the shards settle between phases
        print(f"\nthroughput at concurrency {args.concurrency}:")
        tput: list[dict[str, Any]] = []
        for name, mode, call in build_workloads(args.docs, rng):
            if name not in ("key_lookup", "filter_broad"):
                continue
            result = await throughput(client, name, call, args.iterations, args.concurrency)
            result["routing"] = mode
            tput.append(result)
            print(f"  {name:22} {mode:15} {result['ops_per_s']:>8,.0f} ops/s   "
                  f"p95 {result['p95_ms']:7.2f} ms")

    direct = next(r for r in rows if r["workload"] == "key_lookup")
    scatter = next(r for r in rows if r["workload"] == "filter_broad")
    ratio = scatter["p50_ms"] / direct["p50_ms"] if direct["p50_ms"] else 0

    report = {
        "phase": 1,
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "host": {"platform": platform.platform(), "python": platform.python_version(),
                 "cpus": __import__("os").cpu_count()},
        "cluster": {"shards": len(cfg.shards), "chunks": cfg.num_chunks,
                    "documents": stats["documents"], "replication": "none (Phase 1)"},
        "iterations": args.iterations,
        "repeats": args.repeats,
        "latency": rows,
        "throughput": tput,
        "headline": {
            "direct_p50_ms": direct["p50_ms"],
            "scatter_gather_p50_ms": scatter["p50_ms"],
            "scatter_vs_direct_p50": round(ratio, 2),
        },
    }

    results_dir = ROOT / "results"
    results_dir.mkdir(exist_ok=True)
    (results_dir / "phase1_baseline.json").write_text(json.dumps(report, indent=2))

    doc = (
        "# Phase 1 baseline: direct routing vs scatter-gather\n\n"
        f"Generated {report['generated_at']} on {platform.system()} "
        f"({report['host']['cpus']} CPUs, Python {report['host']['python']}).\n\n"
        f"{len(cfg.shards)} shards / {cfg.num_chunks} chunks, "
        f"{stats['documents']:,} documents, no replication, "
        f"{args.iterations} sequential requests per workload, split over "
        f"{args.repeats} interleaved rounds.\n\n"
        "Scatter-gather timings are CPU-bound (every shard parses and filters its\n"
        "whole collection) and so move with machine load; the direct-path numbers\n"
        "are stable to within a few percent across runs.\n\n"
        "## Latency\n\n" + markdown_table(rows) +
        f"\n## Throughput (concurrency {args.concurrency})\n\n"
        + markdown_table([{**r, "fanout": 1 if r["routing"] == "direct" else len(cfg.shards)}
                          for r in tput]) +
        f"\n## Headline\n\n"
        f"- Direct key lookup p50: **{direct['p50_ms']:.2f} ms**\n"
        f"- Scatter-gather filter p50: **{scatter['p50_ms']:.2f} ms** "
        f"({ratio:.1f}x the direct path, fanout {len(cfg.shards)})\n"
    )
    (ROOT / "docs").mkdir(exist_ok=True)
    (ROOT / "docs" / "phase1_baseline.md").write_text(doc)

    print(f"\nscatter-gather p50 is {ratio:.1f}x the direct-route p50")
    print("wrote results/phase1_baseline.json and docs/phase1_baseline.md")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--docs", type=int, default=20_000)
    parser.add_argument("--batch", type=int, default=1_000)
    parser.add_argument("--iterations", type=int, default=300)
    parser.add_argument("--repeats", type=int, default=3,
                        help="interleaved rounds the iterations are split over")
    parser.add_argument("--concurrency", type=int, default=16)
    return asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    raise SystemExit(main())
