package com.dream11.shardwizard.router.impl;

import com.dream11.shardwizard.router.ShardRouter;
import java.util.List;

// This assumes that routeKey is a number
public class ModuloRouter implements ShardRouter {

  List<Long> shardIds;

  @Override
  public void initialize(List<Long> shardIds) {
    this.shardIds = shardIds;
  }

  @Override
  public long getRoutedShardId(String routeKey) {
    if (shardIds == null || shardIds.isEmpty()) {
      throw new IllegalStateException("No active shards found for entity");
    }
    int shardIndex = (int) (Long.parseLong(routeKey) % shardIds.size());
    return shardIds.get(shardIndex);
  }
}
