package com.dream11.shardwizard.shardmanager;

import static com.dream11.shardwizard.constant.Constants.SHARD_MANAGER_CONFIG_FOLDER;

import com.dream11.shardwizard.config.S3Config;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.config.SqlConfig;
import com.dream11.shardwizard.exception.DatabaseTypeNotFoundException;
import com.dream11.shardwizard.shardmanager.impl.postgres.ShardManagerClientImplPostgres;
import com.dream11.shardwizard.shardmanager.impl.s3.ShardManagerClientImplS3;
import com.dream11.shardwizard.shardmanager.impl.s3.credentials.CustomCredentialS3ClientStrategy;
import com.dream11.shardwizard.shardmanager.impl.s3.credentials.IamRoleS3ClientStrategy;
import com.dream11.shardwizard.shardmanager.impl.s3.credentials.S3ClientStrategy;
import com.dream11.shardwizard.utils.ConfigUtils;
import io.vertx.reactivex.core.Vertx;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class ShardManagerFactory {

  private static ShardManagerClient instance;

  private ShardManagerFactory() {}

  private static ShardManagerClient createPostgresClient(Vertx vertx, SqlConfig sqlConfig) {
    return new ShardManagerClientImplPostgres(vertx, sqlConfig);
  }

  private static ShardManagerClient createS3Client(Vertx vertx, S3Config s3Config) {
    S3ClientStrategy strategy;
    if (StringUtils.isBlank(s3Config.getAccessKey())
        && StringUtils.isBlank(s3Config.getSecretKey())) {
      strategy = new IamRoleS3ClientStrategy();
    } else {
      strategy = new CustomCredentialS3ClientStrategy();
    }
    return new ShardManagerClientImplS3(vertx, s3Config, strategy);
  }

  public static ShardManagerClient getShardManagerClient(Vertx vertx) {
    if (Objects.isNull(instance)) {
      ShardManagerConfig shardManagerConfig =
          ConfigUtils.fromConfigFile(
              "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/%s.conf", ShardManagerConfig.class);
      instance = getShardManagerClient(vertx, shardManagerConfig);
    }
    return instance;
  }

  public static ShardManagerClient getShardManagerClient(
      Vertx vertx, ShardManagerConfig shardManagerConfig) {
    if (Objects.isNull(instance)) {
      String sourceType = shardManagerConfig.getSourceType();
      switch (sourceType) {
        case "POSTGRES":
          instance =
              createPostgresClient(
                  vertx, (SqlConfig) shardManagerConfig.convertToSourceConfig(sourceType));
          break;
        case "S3":
          instance =
              createS3Client(
                  vertx, (S3Config) shardManagerConfig.convertToSourceConfig(sourceType));
          break;
        default:
          throw new DatabaseTypeNotFoundException(sourceType);
      }
    }
    return instance;
  }
}
