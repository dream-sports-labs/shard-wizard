package com.dream11.shardwizard.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
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
}