package com.dream11.shardwizard.metric.event;

import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.dto.ObservabilityEvent;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;

public class DatabaseEventBuilder {
  private String eventName;
  private ObservabilityEvent.EventSeverity severity;
  private RdsCluster cluster;
  private String query;
  private Throwable error;
  private final Map<String, String> additionalTags = new HashMap<>();

  private DatabaseEventBuilder() {}

  public static DatabaseEventBuilder create() {
    return new DatabaseEventBuilder();
  }

  public DatabaseEventBuilder eventName(String eventName) {
    this.eventName = eventName;
    return this;
  }

  public DatabaseEventBuilder severity(ObservabilityEvent.EventSeverity severity) {
    this.severity = severity;
    return this;
  }

  public DatabaseEventBuilder cluster(RdsCluster cluster) {
    this.cluster = cluster;
    return this;
  }

  public DatabaseEventBuilder query(String query) {
    this.query = query;
    return this;
  }

  public DatabaseEventBuilder error(Throwable error) {
    this.severity = ObservabilityEvent.EventSeverity.ERROR;
    this.error = error;
    return this;
  }

  public DatabaseEventBuilder addTag(String key, String value) {
    this.additionalTags.put(key, value);
    return this;
  }

  public ObservabilityEvent build() {
    validateRequiredFields();

    StringBuilder description =
        new StringBuilder()
            .append("Event name: ")
            .append(eventName)
            .append(", status: ")
            .append(severity);

    Map<String, String> tags = new HashMap<>(additionalTags);
    tags.put("eventName", eventName);

    if (cluster != null) {
      description.append(", cluster: ").append(cluster.name());
      tags.put("cluster", cluster.name());
    }

    if (query != null) {
      description.append(", query: ").append(query);
    }

    if (error != null) {
      description.append(", Error: ").append(error.getMessage());
      tags.put("error.message", error.getMessage());
    }

    return ObservabilityEvent.builder()
        .title(eventName)
        .description(description.toString())
        .severity(severity)
        .tags(tags)
        .build();
  }

  private void validateRequiredFields() {
    if (StringUtils.isEmpty(eventName) || Objects.isNull(severity)) {
      throw new IllegalStateException("Event name and severity are required");
    }
  }
}
