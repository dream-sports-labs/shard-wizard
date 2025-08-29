package com.dream11.shardwizard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.typesafe.config.Optional;
import java.util.Map;
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

  private Integer maxConnections = 10;

  private Integer maxWaitQueueSize = 50;

  private Integer connectionTimeoutMs = 500;

  private String username;

  private String password;

  private String database;

  @Optional private String endpoint;
  @Optional private String region;

  @Optional private CircuitBreakerConfigDTO circuitBreaker;
  @Optional private Map<String, Object> tableConnectionMap; // todo - can be removed
  @Optional private String accessKey;
  @Optional private String secretKey;

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TableConnectionInfo {
    private String endpoint;
    private String region;
  }
}
