package com.dream11.shardwizard.config;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class MetricAgentConfig {
  private DatadogConfig datadog;
}
