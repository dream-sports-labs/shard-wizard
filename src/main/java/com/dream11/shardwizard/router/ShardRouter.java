package com.dream11.shardwizard.router;

import java.util.List;

public interface ShardRouter {

  /**
   * @param shardIds List of shardIds to be used for initializing the Router. This method will be
   *     called only once during the Router creation
   */
  void initialize(List<Long> shardIds);

  /**
   * @param routeKey key which will be used for deciding the routed shard
   * @return ShardId to which the routeKey will be routed
   */
  long getRoutedShardId(String routeKey);
}
