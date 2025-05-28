package com.dream11.shardwizard.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DefaultShardConfig {

  private int port;
  private String database;
  private String username;
  private String password;
  private int maxConnections;
}