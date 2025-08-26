package com.dream11.shardwizard.metric;

import java.util.HashMap;
import java.util.Map;

public abstract class ObservabilityAdapter implements ObservabilityService {
  protected final String serviceName;
  protected final Map<String, String> defaultTags;

  protected ObservabilityAdapter(String serviceName, Map<String, String> defaultTags) {
    this.serviceName = serviceName;
    this.defaultTags = defaultTags != null ? new HashMap<>(defaultTags) : new HashMap<>();
  }

  protected Map<String, String> mergeTags(Map<String, String> eventTags) {
    Map<String, String> merged = new HashMap<>(defaultTags);
    if (eventTags != null) {
      merged.putAll(eventTags);
    }
    merged.put("service", serviceName);
    return merged;
  }

  protected String formatMetricName(String name) {
    return serviceName.toLowerCase() + "." + name;
  }
}
