Phase 2 patch for MongoDB-Inspired Sharded Document Store

Copy the contents of:
  shardstore/ -> your project's shardstore/
  scripts/   -> your project's scripts/
  config/    -> your project's config/

This patch adds follower replication, follower read preference, ownership
epochs, automatic follower promotion, stale-primary fencing, and recovery
metrics. See docs/phase2.md for commands and limitations.

Back up your existing data/ directory before testing. --reset deletes shard
and follower databases.
