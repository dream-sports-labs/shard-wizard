package com.dream11.shardwizard.shardmanager.impl.s3.credentials;

import com.dream11.shardwizard.config.S3Config;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public interface S3ClientStrategy {

  S3AsyncClient createClient(S3Config config);
}
