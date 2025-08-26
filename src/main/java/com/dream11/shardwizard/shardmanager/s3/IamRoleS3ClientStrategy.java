package com.dream11.shardwizard.shardmanager.s3;

import com.dream11.shardwizard.config.S3Config;
import java.time.Duration;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

public class IamRoleS3ClientStrategy implements S3ClientStrategy {
  private static final int READ_TIMEOUT = 8;
  private static final int CONNECTION_TIMEOUT = 5;
  private static final int MAX_CONCURRENCY = 5;
  private static final int MAX_RETRIES = 2;
  private static final int API_CALL_TIMEOUT = 40;

  @Override
  public S3AsyncClient createClient(S3Config config) {
    SdkAsyncHttpClient httpClient = createHttpClient();
    ClientOverrideConfiguration overrideConfig = createOverrideConfiguration();

    return S3AsyncClient.builder()
        .region(Region.of(config.getRegion()))
        .credentialsProvider(DefaultCredentialsProvider.create())
        .httpClient(httpClient)
        .overrideConfiguration(overrideConfig)
        .build();
  }

  private SdkAsyncHttpClient createHttpClient() {
    return NettyNioAsyncHttpClient.builder()
        .readTimeout(Duration.ofSeconds(READ_TIMEOUT))
        .connectionTimeout(Duration.ofSeconds(CONNECTION_TIMEOUT))
        .maxConcurrency(MAX_CONCURRENCY)
        .build();
  }

  private ClientOverrideConfiguration createOverrideConfiguration() {
    return ClientOverrideConfiguration.builder()
        .apiCallTimeout(Duration.ofSeconds(API_CALL_TIMEOUT))
        .retryPolicy(createRetryPolicy())
        .build();
  }

  private RetryPolicy createRetryPolicy() {
    return RetryPolicy.builder(RetryMode.STANDARD).numRetries(MAX_RETRIES).build();
  }
}
