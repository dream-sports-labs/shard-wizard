package com.dream11.shardwizard.config;

import com.typesafe.config.Optional;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@EqualsAndHashCode(callSuper = false)
@NoArgsConstructor
public class DynamoConfig extends SourceConfig {

  // Default HTTP client configuration values
  public static final int DEFAULT_CONNECTION_TIMEOUT_MS = 1000;
  public static final int DEFAULT_CONNECTION_MAX_IDLE_TIME_MS = 15000;
  public static final int DEFAULT_MAX_CONCURRENCY = 5000;
  public static final int DEFAULT_KEEP_ALIVE_INTERVAL_MS = 150;
  public static final int DEFAULT_KEEP_ALIVE_TIMEOUT_MS = 500;

  @NonNull private String region;
  @NonNull private String accessKey;
  @NonNull private String secretKey;
  private String endpointOverride;

  // HTTP client configuration properties
  @Optional private Integer connectionTimeoutMs = DEFAULT_CONNECTION_TIMEOUT_MS;
  @Optional private Integer connectionMaxIdleTimeMs = DEFAULT_CONNECTION_MAX_IDLE_TIME_MS;
  @Optional private Integer maxConcurrency = DEFAULT_MAX_CONCURRENCY;
  @Optional private Integer keepAliveIntervalMs = DEFAULT_KEEP_ALIVE_INTERVAL_MS;
  @Optional private Integer keepAliveTimeoutMs = DEFAULT_KEEP_ALIVE_TIMEOUT_MS;
}
