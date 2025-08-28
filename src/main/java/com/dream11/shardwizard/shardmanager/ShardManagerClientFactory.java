package com.dream11.shardwizard.shardmanager;

import com.dream11.shardwizard.config.ShardManagerConfig;
import io.vertx.reactivex.core.Vertx;

public interface ShardManagerClientFactory {
  ShardManagerClient createClient(Vertx vertx, ShardManagerConfig shardManagerConfig);
}
