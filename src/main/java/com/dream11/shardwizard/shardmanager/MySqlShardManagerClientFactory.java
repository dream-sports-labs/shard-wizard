package com.dream11.shardwizard.shardmanager;

import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.config.SqlConfig;
import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.shardmanager.impl.ShardManagerClientImplMySql;
import io.vertx.reactivex.core.Vertx;

public class MySqlShardManagerClientFactory implements ShardManagerClientFactory {
  @Override
  public ShardManagerClient createClient(Vertx vertx, ShardManagerConfig shardManagerConfig) {
    SqlConfig sqlConfig =
        (SqlConfig) shardManagerConfig.convertToSourceConfig(DatabaseType.MYSQL.name());
    return new ShardManagerClientImplMySql(vertx, sqlConfig);
  }
}
