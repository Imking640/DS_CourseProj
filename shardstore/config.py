"""Static cluster topology, including one follower per logical shard."""

from __future__ import annotations

import json
from pathlib import Path
from pydantic import BaseModel, Field

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "cluster.json"


class Endpoint(BaseModel):
    host: str = "127.0.0.1"
    port: int

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"


class ReplicaSpec(Endpoint):
    shard_id: str
    data_path: str


class ShardSpec(Endpoint):
    shard_id: str
    data_path: str
    follower: ReplicaSpec | None = None


class ClusterConfig(BaseModel):
    cluster_id: str = "ds-shard-store"
    shard_key: str = "_id"
    shard_map_path: str = "data/shardmap.db"
    num_chunks: int = 8
    router: Endpoint
    shards: list[ShardSpec] = Field(min_length=1)

    @classmethod
    def load(cls, path: str | Path | None = None) -> "ClusterConfig":
        config_path = Path(path) if path else DEFAULT_CONFIG_PATH
        return cls.model_validate(json.loads(config_path.read_text()))

    def resolve(self, relative: str) -> Path:
        candidate = Path(relative)
        return candidate if candidate.is_absolute() else PROJECT_ROOT / candidate

    @property
    def shard_map_file(self) -> Path:
        return self.resolve(self.shard_map_path)

    def shard(self, shard_id: str) -> ShardSpec:
        for spec in self.shards:
            if spec.shard_id == shard_id:
                return spec
        raise KeyError(f"unknown primary shard {shard_id!r}")

    def all_nodes(self):
        """Return primary and follower process configurations."""
        nodes = []
        for primary in self.shards:
            follower = primary.follower
            nodes.append({
                "shard_id": primary.shard_id,
                "host": primary.host,
                "port": primary.port,
                "data_path": primary.data_path,
                "role": "primary",
                "partner_id": follower.shard_id if follower else None,
                "partner_url": follower.base_url if follower else None,
            })
            if follower:
                nodes.append({
                    "shard_id": follower.shard_id,
                    "host": follower.host,
                    "port": follower.port,
                    "data_path": follower.data_path,
                    "role": "follower",
                    "partner_id": primary.shard_id,
                    "partner_url": primary.base_url,
                })
        return nodes
