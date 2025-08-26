package com.dream11.shardwizard.metric.impl;

import com.dream11.shardwizard.dto.ObservabilityEvent;
import com.dream11.shardwizard.metric.ObservabilityAdapter;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

public class NoOpObservabilityAdapter extends ObservabilityAdapter {

  public NoOpObservabilityAdapter() {
    super(StringUtils.EMPTY, Map.of());
  }

  @Override
  public void recordMetric(String name, double value, Map<String, String> tags) {}

  @Override
  public void recordEvent(ObservabilityEvent event) {}

  @Override
  public void recordServiceHealth(
      String serviceName, ObservabilityEvent.HealthStatus status, Map<String, String> metadata) {}

  @Override
  public void recordTrace(String operationName, long durationMs, Map<String, String> context) {}
}
