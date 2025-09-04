package com.dream11.shardwizard.constant;

/** Enumeration of available shard routing strategies. */
public enum RouterType {
  /**
   * Uses modulo operation to distribute data across shards. Simple and fast but doesn't handle
   * shard changes well.
   */
  MODULO,

  /**
   * Uses consistent hashing to distribute data across shards. Better for handling shard
   * additions/removals with minimal data movement.
   */
  CONSISTENT
}
