package com.dream11.shardwizard.dto;

import java.util.Map;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class ObservabilityEvent {
  private final String title;
  private final String description;
  private final EventSeverity severity;
  private final Map<String, String> tags;
  private final long timestamp;

  public enum EventSeverity {
    INFO,
    WARNING,
    ERROR,
    CRITICAL
  }

  public enum HealthStatus {
    HEALTHY,
    DEGRADED,
    UNHEALTHY,
    UNKNOWN
  }
}
