package com.dream11.shardwizard.shardmanager.s3;

import com.dream11.shardwizard.config.S3Config;
import java.net.URI;
import org.apache.commons.lang3.StringUtils;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class CustomCredentialS3ClientStrategy implements S3ClientStrategy {
  @Override
  public S3AsyncClient createClient(S3Config config) {
    AwsBasicCredentials credentials =
        AwsBasicCredentials.create(config.getAccessKey(), config.getSecretKey());
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(credentials);

    return S3AsyncClient.builder()
        .endpointOverride(
            StringUtils.isNotBlank(config.getEndpointOverride())
                ? URI.create(config.getEndpointOverride())
                : null)
        .region(Region.of(config.getRegion()))
        .credentialsProvider(credentialsProvider)
        .forcePathStyle(config.isForcePathStyle())
        .build();
  }
}
