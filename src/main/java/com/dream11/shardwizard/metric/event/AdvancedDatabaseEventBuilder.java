package com.dream11.shardwizard.metric.event;

import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.dto.ObservabilityEvent;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/*
* Advanced Database Event Builder for Complex Scenarios
* Example usage -
*
DatabaseEventRecorder.getInstance().recordEvent(
    AdvancedDatabaseEventBuilder.create()
        .eventName(DB_QUERY)
        .severity(ObservabilityEvent.EventSeverity.INFO)
        .cluster(cluster)
        .query(query)
        .executionTime(150L)
        .rowsAffected(5)
        .addCustomTag("batch_size", "100")
        .build()
);
* */
public class AdvancedDatabaseEventBuilder {
  private String eventName;
  private ObservabilityEvent.EventSeverity severity;
  private RdsCluster cluster;
  private String query;
  private Throwable error;
  private Long executionTimeMs;
  private Integer rowsAffected;
  private String connectionId;
  private Instant timestamp;
  private final Map<String, String> customTags = new HashMap<>();

  private AdvancedDatabaseEventBuilder() {}

  public static AdvancedDatabaseEventBuilder create() {
    return new AdvancedDatabaseEventBuilder();
  }

  public AdvancedDatabaseEventBuilder eventName(String eventName) {
    this.eventName = eventName;
    return this;
  }

  public AdvancedDatabaseEventBuilder severity(ObservabilityEvent.EventSeverity severity) {
    this.severity = severity;
    return this;
  }

  public AdvancedDatabaseEventBuilder cluster(RdsCluster cluster) {
    this.cluster = cluster;
    return this;
  }

  public AdvancedDatabaseEventBuilder query(String query) {
    this.query = query;
    return this;
  }

  public AdvancedDatabaseEventBuilder error(Throwable error) {
    this.error = error;
    if (this.severity == null) {
      this.severity = ObservabilityEvent.EventSeverity.ERROR;
    }
    return this;
  }

  public AdvancedDatabaseEventBuilder executionTime(Long executionTimeMs) {
    this.executionTimeMs = executionTimeMs;
    return this;
  }

  public AdvancedDatabaseEventBuilder rowsAffected(Integer rowsAffected) {
    this.rowsAffected = rowsAffected;
    return this;
  }

  public AdvancedDatabaseEventBuilder connectionId(String connectionId) {
    this.connectionId = connectionId;
    return this;
  }

  public AdvancedDatabaseEventBuilder timestamp(Instant timestamp) {
    this.timestamp = timestamp;
    return this;
  }

  public AdvancedDatabaseEventBuilder addCustomTag(String key, String value) {
    this.customTags.put(key, value);
    return this;
  }

  public ObservabilityEvent build() {
    validateRequiredFields();

    StringBuilder description = buildDescription();
    Map<String, String> tags = buildTags();

    return ObservabilityEvent.builder()
        .title(eventName)
        .description(description.toString())
        .severity(severity)
        .tags(tags)
        .build();
  }

  private StringBuilder buildDescription() {
    StringBuilder description =
        new StringBuilder()
            .append("Event name: ")
            .append(eventName)
            .append(", status: ")
            .append(severity);

    if (cluster != null) {
      description.append(", cluster: ").append(cluster.name());
    }

    if (query != null) {
      description.append(", query: ").append(query);
    }

    if (executionTimeMs != null) {
      description.append(", execution_time_ms: ").append(executionTimeMs);
    }

    if (rowsAffected != null) {
      description.append(", rows_affected: ").append(rowsAffected);
    }

    if (error != null) {
      description.append(", Error: ").append(error.getMessage());
    }

    return description;
  }

  private Map<String, String> buildTags() {
    Map<String, String> tags = new HashMap<>(customTags);
    tags.put("eventName", eventName);

    if (cluster != null) {
      tags.put("cluster", cluster.name());
    }

    if (error != null) {
      tags.put("error.message", error.getMessage());
      tags.put("error.class", error.getClass().getSimpleName());
    }

    if (executionTimeMs != null) {
      tags.put("execution_time_ms", executionTimeMs.toString());
    }

    if (rowsAffected != null) {
      tags.put("rows_affected", rowsAffected.toString());
    }

    if (connectionId != null) {
      tags.put("connection_id", connectionId);
    }

    if (timestamp != null) {
      tags.put("timestamp", timestamp.toString());
    }

    return tags;
  }

  private void validateRequiredFields() {
    if (eventName == null || severity == null) {
      throw new IllegalStateException("Event name and severity are required");
    }
  }
}
