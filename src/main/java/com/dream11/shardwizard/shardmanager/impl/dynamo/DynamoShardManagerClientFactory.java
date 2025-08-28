package com.dream11.shardwizard.shardmanager.impl.dynamo;

import com.dream11.shardwizard.config.DynamoConfig;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.dream11.shardwizard.shardmanager.ShardManagerClientFactory;
import io.vertx.reactivex.core.Vertx;

public class DynamoShardManagerClientFactory implements ShardManagerClientFactory {
  @Override
  public ShardManagerClient createClient(Vertx vertx, ShardManagerConfig shardManagerConfig) {
    DynamoConfig dynamoConfig =
        (DynamoConfig) shardManagerConfig.convertToSourceConfig(DatabaseType.DYNAMO.name());
    return new ShardManagerClientImplDynamo(dynamoConfig);
  }
}
