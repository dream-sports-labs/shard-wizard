package com.dream11.shardwizard.shardmanager.impl;

import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.constant.Constants;
import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.exception.DatabaseTypeNotFoundException;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.dream11.shardwizard.shardmanager.ShardManagerClientFactory;
import com.dream11.shardwizard.shardmanager.impl.dynamo.DynamoShardManagerClientFactory;
import com.dream11.shardwizard.shardmanager.impl.mysql.MysqlShardManagerClientFactory;
import com.dream11.shardwizard.shardmanager.impl.postgres.PostgresShardManagerClientFactory;
import com.dream11.shardwizard.shardmanager.impl.s3.S3ShardManagerClientFactory;
import com.dream11.shardwizard.utils.ConfigUtils;
import io.vertx.reactivex.core.Vertx;
import java.util.EnumMap;
import java.util.Map;
import java.util.Objects;

public class ShardManagerFactory {

  private ShardManagerFactory() {}

  private static ShardManagerClient instance;

  private static final Map<DatabaseType, ShardManagerClientFactory> factoryMap =
      new EnumMap<>(DatabaseType.class);

  static {
    factoryMap.put(DatabaseType.MYSQL, new MysqlShardManagerClientFactory());
    factoryMap.put(DatabaseType.POSTGRES, new PostgresShardManagerClientFactory());
    factoryMap.put(DatabaseType.S3, new S3ShardManagerClientFactory());
    factoryMap.put(DatabaseType.DYNAMO, new DynamoShardManagerClientFactory());
  }

  public static ShardManagerClient getShardManagerClient(Vertx vertx) {
    if (Objects.isNull(instance)) {
      ShardManagerConfig shardManagerConfig =
          ConfigUtils.fromConfigFile(
              "config/" + Constants.SHARD_MANAGER_CONFIG_FOLDER + "/%s.conf",
              ShardManagerConfig.class);
      instance = getShardManagerClient(vertx, shardManagerConfig);
    }
    return instance;
  }

  public static ShardManagerClient getShardManagerClient(
      Vertx vertx, ShardManagerConfig shardManagerConfig) {
    if (Objects.isNull(instance)) {
      DatabaseType sourceType = shardManagerConfig.getSourceType();
      ShardManagerClientFactory factory = factoryMap.get(sourceType);
      if (factory == null) {
        throw new DatabaseTypeNotFoundException(sourceType.name());
      }
      instance = factory.createClient(vertx, shardManagerConfig);
    }
    return instance;
  }
}
