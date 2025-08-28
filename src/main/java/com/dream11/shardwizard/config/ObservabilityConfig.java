package com.dream11.shardwizard.config;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ObservabilityConfig {
  private String provider;
  private String serviceName;
  private Map<String, String> defaultTags;
  private DatadogConfig datadogConfig;
}
