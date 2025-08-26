package com.dream11.shardwizard.metric;

import com.dream11.shardwizard.model.ObservabilityEvent;
import com.dream11.shardwizard.model.ObservabilityEvent.*;
import java.util.Map;

public interface ObservabilityService {
  void recordMetric(String name, double value, Map<String, String> tags);

  void recordEvent(ObservabilityEvent event);

  void recordServiceHealth(String serviceName, HealthStatus status, Map<String, String> metadata);

  void recordTrace(String operationName, long durationMs, Map<String, String> context);
}
