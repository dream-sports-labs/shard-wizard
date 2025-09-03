package com.dream11.shardwizard.model;

import static com.dream11.shardwizard.config.DynamoConfig.DEFAULT_CONNECTION_MAX_IDLE_TIME_MS;
import static com.dream11.shardwizard.config.DynamoConfig.DEFAULT_CONNECTION_TIMEOUT_MS;
import static com.dream11.shardwizard.config.DynamoConfig.DEFAULT_KEEP_ALIVE_INTERVAL_MS;
import static com.dream11.shardwizard.config.DynamoConfig.DEFAULT_KEEP_ALIVE_TIMEOUT_MS;
import static com.dream11.shardwizard.config.DynamoConfig.DEFAULT_MAX_CONCURRENCY;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.typesafe.config.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
@Data
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardConnectionParameters {

  private String writerHost;

  private String readerHost;

  private Integer port;

  @Builder.Default private Integer maxConnections = 10;

  @Builder.Default private Integer maxWaitQueueSize = 50;

  @Builder.Default private Integer connectionTimeoutMs = 500;

  private String username;

  private String password;

  private String database;

  @Optional private String endpoint;
  @Optional private String region;

  @Optional private CircuitBreakerConfigDTO circuitBreaker;
  @Optional private String accessKey;
  @Optional private String secretKey;

  // DynamoDB HTTP client configuration properties
  @Optional @Builder.Default
  private Integer dynamoConnectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;

  @Optional @Builder.Default
  private Integer dynamoConnectionMaxIdleTimeMs = DEFAULT_CONNECTION_MAX_IDLE_TIME_MS;

  @Optional @Builder.Default private Integer dynamoMaxConcurrency = DEFAULT_MAX_CONCURRENCY;

  @Optional @Builder.Default
  private Integer dynamoKeepAliveIntervalMs = DEFAULT_KEEP_ALIVE_INTERVAL_MS;

  @Optional @Builder.Default
  private Integer dynamoKeepAliveTimeoutMs = DEFAULT_KEEP_ALIVE_TIMEOUT_MS;
}
