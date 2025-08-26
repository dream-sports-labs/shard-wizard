package com.dream11.shardwizard.shardmanager;

import com.dream11.shardwizard.config.S3Config;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.shardmanager.impl.ShardManagerClientImplS3;
import com.dream11.shardwizard.shardmanager.s3.CustomCredentialS3ClientStrategy;
import com.dream11.shardwizard.shardmanager.s3.IamRoleS3ClientStrategy;
import com.dream11.shardwizard.shardmanager.s3.S3ClientStrategy;
import io.vertx.reactivex.core.Vertx;
import org.apache.commons.lang3.StringUtils;

public class S3ShardManagerClientFactory implements ShardManagerClientFactory {
  @Override
  public ShardManagerClient createClient(Vertx vertx, ShardManagerConfig shardManagerConfig) {
    S3Config s3Config = (S3Config) shardManagerConfig.convertToSourceConfig(DatabaseType.S3.name());
    S3ClientStrategy strategy =
        StringUtils.isBlank(s3Config.getAccessKey()) && StringUtils.isBlank(s3Config.getSecretKey())
            ? new IamRoleS3ClientStrategy()
            : new CustomCredentialS3ClientStrategy();
    return new ShardManagerClientImplS3(vertx, s3Config, strategy);
  }
}
