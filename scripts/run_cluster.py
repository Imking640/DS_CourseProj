#!/usr/bin/env python3
"""Launch four primary shards, four followers and the query router."""
from __future__ import annotations
import argparse
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from shardstore.config import ClusterConfig
from shardstore.shardmap import ShardMap

LOG_DIR = ROOT / "logs"
PID_FILE = LOG_DIR / "cluster.pid"


def python_bin() -> str:
    venv = ROOT / ".venv" / "bin" / "python"
    return str(venv) if venv.exists() else sys.executable


def wait_until_healthy(url: str, timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=1.0) as resp:
                if resp.status == 200:
                    return True
        except (urllib.error.URLError, OSError):
            time.sleep(0.15)
    return False


def post_json(url: str, timeout: float = 30.0) -> dict:
    request = urllib.request.Request(url, data=b"{}",
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode())


def spawn(name: str, args: list[str], env: dict[str, str]) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / f"{name}.log", "w")
    return subprocess.Popen(args, cwd=ROOT, env={**os.environ, **env},
                            stdout=log, stderr=subprocess.STDOUT)


def stop_running_cluster() -> int:
    if not PID_FILE.exists():
        print("no cluster.pid; nothing to stop")
        return 0
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        print("cluster launcher was already stopped")
        return 0
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    PID_FILE.unlink(missing_ok=True)
    print(f"stopped cluster launcher {pid}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--reset", action="store_true",
                        help="delete all primary/follower databases and rebuild the map")
    parser.add_argument("--log-level", default="warning")
    parser.add_argument("--stop", action="store_true")
    args = parser.parse_args()
    if args.stop:
        return stop_running_cluster()

    cfg = ClusterConfig.load(args.config)
    nodes = cfg.all_nodes()
    if args.reset:
        for node in nodes:
            db = cfg.resolve(node["data_path"])
            for suffix in ("", "-wal", "-shm", ".node-state.db"):
                Path(str(db) + suffix).unlink(missing_ok=True)
        for suffix in ("", "-wal", "-shm"):
            Path(str(cfg.shard_map_file) + suffix).unlink(missing_ok=True)
        print("reset: all primary/follower databases and shard map removed")

    primary_tuples = [(s.shard_id, s.host, s.port) for s in cfg.shards]
    follower_tuples = [
        (s.follower.shard_id, s.follower.host, s.follower.port, s.shard_id)
        for s in cfg.shards if s.follower
    ]
    with ShardMap(cfg.shard_map_file) as smap:
        existing = smap.snapshot() if smap.is_initialised else None
        if not existing or not existing.replicas:
            # Rebuild only metadata; primary document databases are retained.
            smap.bootstrap(primary_tuples, cfg.num_chunks, cfg.shard_key,
                           force=bool(existing), followers=follower_tuples)
            print("shard map initialised with primary/follower pairs")
        else:
            print(f"existing replica-aware shard map retained (version {smap.version})")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))
    procs: list[tuple[str, subprocess.Popen]] = []

    def shutdown(*_: object) -> None:
        for _, proc in reversed(procs):
            if proc.poll() is None:
                proc.terminate()
        for _, proc in procs:
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
        PID_FILE.unlink(missing_ok=True)
        print("\ncluster stopped")

    signal.signal(signal.SIGINT, lambda *_: (shutdown(), sys.exit(0)))
    signal.signal(signal.SIGTERM, lambda *_: (shutdown(), sys.exit(0)))

    # Launch followers first so primaries can immediately replicate to them.
    ordered = sorted(nodes, key=lambda n: 0 if n["role"] == "follower" else 1)
    for node in ordered:
        env = {
            "SHARD_ID": node["shard_id"],
            "SHARD_DB": str(cfg.resolve(node["data_path"])),
            "SHARD_KEY": cfg.shard_key,
            "SHARD_ROLE": node["role"],
            "PARTNER_URL": node["partner_url"] or "",
            "SHARD_MAP_PATH": str(cfg.shard_map_file),
            "SHARD_EPOCH": "1",
        }
        proc = spawn(node["shard_id"],
            [python_bin(), "-m", "uvicorn", "shardstore.shard_node:app_from_env",
             "--factory", "--host", node["host"], "--port", str(node["port"]),
             "--log-level", args.log_level], env)
        procs.append((node["shard_id"], proc))

    for node in ordered:
        url = f"http://{node['host']}:{node['port']}"
        ok = wait_until_healthy(url)
        print(f"  {node['shard_id']:20} {url} {'ready' if ok else 'FAILED'}")
        if not ok:
            shutdown()
            print(f"see logs/{node['shard_id']}.log", file=sys.stderr)
            return 1

    # Copy existing primary contents to followers before accepting router traffic.
    with ShardMap(cfg.shard_map_file) as current_map:
        active_replicas = current_map.snapshot().replicas
    for primary in cfg.shards:
        if primary.follower and active_replicas.get(primary.shard_id, {}).get("active_id") == primary.shard_id:
            try:
                result = post_json(f"{primary.base_url}/internal/sync-follower")
                print(f"  synced {primary.shard_id} -> {primary.follower.shard_id}: "
                      f"{result['synchronised']} documents")
            except Exception as exc:
                shutdown()
                print(f"initial follower sync failed for {primary.shard_id}: {exc}",
                      file=sys.stderr)
                return 1

    router = spawn("router",
        [python_bin(), "-m", "uvicorn", "shardstore.router:app_from_env",
         "--factory", "--host", cfg.router.host, "--port", str(cfg.router.port),
         "--log-level", args.log_level],
        {"SHARD_MAP": str(cfg.shard_map_file)})
    procs.append(("router", router))
    if not wait_until_healthy(cfg.router.base_url):
        shutdown()
        print("router failed to start; see logs/router.log", file=sys.stderr)
        return 1
    print(f"  {'router':20} {cfg.router.base_url} ready")
    print("\ncluster up -- Ctrl-C to stop")
    try:
        reported = set()
        while True:
            time.sleep(0.5)
            for name, proc in procs:
                if proc.poll() is not None and name not in reported:
                    reported.add(name)
                    print(f"{name} exited with code {proc.returncode}", file=sys.stderr)
                    # A primary crash is an expected Phase 2 event. Keep the
                    # router and follower alive so automatic promotion can run.
                    if name == "router":
                        shutdown()
                        return 1
    except KeyboardInterrupt:
        shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
