package com.dream11.shardwizard.config;

import lombok.Data;

@Data
public class DatadogConfig {
  private String host;
  private int port;
  private String prefix;
}
