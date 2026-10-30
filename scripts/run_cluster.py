#!/usr/bin/env python3
"""Start the whole cluster: one uvicorn process per shard, plus the router.

    python scripts/run_cluster.py            # start and stay in the foreground
    python scripts/run_cluster.py --reset    # wipe shard data and the map first
    python scripts/run_cluster.py --stop     # stop a cluster started earlier

Each shard is a genuinely separate OS process with its own SQLite file; the
router only ever reaches them over HTTP.  Logs land in logs/<name>.log.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from shardstore.config import ClusterConfig  # noqa: E402
from shardstore.shardmap import ShardMap  # noqa: E402

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


def spawn(name: str, args: list[str], env: dict[str, str]) -> subprocess.Popen:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = open(LOG_DIR / f"{name}.log", "w")
    return subprocess.Popen(
        args, cwd=ROOT, env={**os.environ, **env}, stdout=log, stderr=subprocess.STDOUT
    )


def stop_running_cluster() -> int:
    if not PID_FILE.exists():
        print("no cluster.pid; nothing to stop")
        return 0
    pid = int(PID_FILE.read_text().strip())
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        print(f"launcher {pid} is already gone")
        PID_FILE.unlink(missing_ok=True)
        return 0
    for _ in range(100):
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            break
        time.sleep(0.1)
    PID_FILE.unlink(missing_ok=True)
    print(f"stopped cluster (launcher pid {pid})")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", default=None)
    parser.add_argument("--reset", action="store_true",
                        help="delete shard databases and rebuild the shard map")
    parser.add_argument("--log-level", default="warning")
    parser.add_argument("--stop", action="store_true",
                        help="stop the cluster recorded in logs/cluster.pid")
    args = parser.parse_args()

    if args.stop:
        return stop_running_cluster()

    cfg = ClusterConfig.load(args.config)

    if args.reset:
        for spec in cfg.shards:
            for suffix in ("", "-wal", "-shm"):
                path = Path(str(cfg.resolve(spec.data_path)) + suffix)
                path.unlink(missing_ok=True)
        for suffix in ("", "-wal", "-shm"):
            Path(str(cfg.shard_map_file) + suffix).unlink(missing_ok=True)
        print("reset: shard databases and shard map removed")

    with ShardMap(cfg.shard_map_file) as smap:
        if not smap.is_initialised:
            smap.bootstrap(
                shards=[(s.shard_id, s.host, s.port) for s in cfg.shards],
                num_chunks=cfg.num_chunks,
                shard_key=cfg.shard_key,
            )
            print(f"shard map bootstrapped with {cfg.num_chunks} chunks")
        print(f"shard map  : {cfg.shard_map_file} (version {smap.version})")

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(os.getpid()))
    procs: list[tuple[str, subprocess.Popen]] = []

    def shutdown(*_: object) -> None:
        for name, proc in reversed(procs):
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

    for spec in cfg.shards:
        proc = spawn(
            spec.shard_id,
            [python_bin(), "-m", "uvicorn", "shardstore.shard_node:app_from_env",
             "--factory", "--host", spec.host, "--port", str(spec.port),
             "--log-level", args.log_level],
            {"SHARD_ID": spec.shard_id,
             "SHARD_DB": str(cfg.resolve(spec.data_path)),
             "SHARD_KEY": cfg.shard_key},
        )
        procs.append((spec.shard_id, proc))

    for spec in cfg.shards:
        ok = wait_until_healthy(spec.base_url)
        print(f"  {spec.shard_id:10} {spec.base_url}  pid {dict(procs)[spec.shard_id].pid}"
              f"  {'ready' if ok else 'FAILED'}")
        if not ok:
            shutdown()
            print(f"see logs/{spec.shard_id}.log", file=sys.stderr)
            return 1

    router = spawn(
        "router",
        [python_bin(), "-m", "uvicorn", "shardstore.router:app_from_env",
         "--factory", "--host", cfg.router.host, "--port", str(cfg.router.port),
         "--log-level", args.log_level],
        {"SHARD_MAP": str(cfg.shard_map_file)},
    )
    procs.append(("router", router))
    if not wait_until_healthy(cfg.router.base_url):
        shutdown()
        print("router failed to start; see logs/router.log", file=sys.stderr)
        return 1
    print(f"  {'router':10} {cfg.router.base_url}  pid {router.pid}  ready")
    print("\ncluster up -- Ctrl-C to stop")

    try:
        while True:
            time.sleep(0.5)
            for name, proc in procs:
                if proc.poll() is not None:
                    print(f"{name} exited with code {proc.returncode}", file=sys.stderr)
                    shutdown()
                    return 1
    except KeyboardInterrupt:
        shutdown()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
