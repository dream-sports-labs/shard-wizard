package com.dream11.shardwizard.metric.impl;

import com.dream11.shardwizard.dto.ObservabilityEvent;
import com.dream11.shardwizard.metric.ObservabilityAdapter;
import com.timgroup.statsd.Event;
import com.timgroup.statsd.NonBlockingStatsDClientBuilder;
import com.timgroup.statsd.ServiceCheck;
import com.timgroup.statsd.StatsDClient;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DatadogObservabilityAdapter extends ObservabilityAdapter {
  private final StatsDClient statsdClient;

  public DatadogObservabilityAdapter(
      String serviceName, Map<String, String> defaultTags, String host, int port, String prefix) {
    super(serviceName, defaultTags);
    this.statsdClient =
        new NonBlockingStatsDClientBuilder().prefix(prefix).hostname(host).port(port).build();
  }

  @Override
  public void recordMetric(String name, double value, Map<String, String> tags) {
    try {
      String[] tagArray = convertToDatadogTags(mergeTags(tags));
      statsdClient.gauge(formatMetricName(name), value, tagArray);
      log.debug("Recorded metric: {} = {} with tags: {}", name, value, tags);
    } catch (Exception e) {
      log.error("Failed to record metric: {}", name, e);
    }
  }

  @Override
  public void recordEvent(ObservabilityEvent event) {
    try {
      Event.AlertType alertType = mapSeverityToDatadogAlertType(event.getSeverity());
      Event ddEvent =
          Event.builder()
              .withTitle(event.getTitle())
              .withText(event.getDescription())
              .withAlertType(alertType)
              .build();

      String[] tags = convertToDatadogTags(mergeTags(event.getTags()));
      statsdClient.recordEvent(ddEvent, tags);
      log.debug("Recorded event: {}", event.getTitle());
    } catch (Exception e) {
      log.error("Failed to record event: {}", event.getTitle(), e);
    }
  }

  @Override
  public void recordServiceHealth(
      String serviceName, ObservabilityEvent.HealthStatus status, Map<String, String> metadata) {
    try {
      ServiceCheck.Status ddStatus = mapHealthStatusToDatadog(status);
      ServiceCheck serviceCheck =
          ServiceCheck.builder()
              .withName(formatMetricName("health_check." + serviceName))
              .withStatus(ddStatus)
              .withMessage(metadata.getOrDefault("message", "Service health check"))
              .build();

      statsdClient.serviceCheck(serviceCheck);
      log.debug("Recorded service health: {} = {}", serviceName, status);
    } catch (Exception e) {
      log.error("Failed to record service health for: {}", serviceName, e);
    }
  }

  @Override
  public void recordTrace(String operationName, long durationMs, Map<String, String> context) {
    try {
      String[] tags = convertToDatadogTags(mergeTags(context));
      statsdClient.histogram(
          formatMetricName("trace." + operationName + ".duration"), durationMs, tags);
      statsdClient.incrementCounter(formatMetricName("trace." + operationName + ".count"), tags);
      log.debug("Recorded trace: {} duration: {}ms", operationName, durationMs);
    } catch (Exception e) {
      log.error("Failed to record trace: {}", operationName, e);
    }
  }

  private String[] convertToDatadogTags(Map<String, String> tags) {
    return tags.entrySet().stream()
        .map(e -> e.getKey() + ":" + e.getValue())
        .toArray(String[]::new);
  }

  private Event.AlertType mapSeverityToDatadogAlertType(ObservabilityEvent.EventSeverity severity) {
    switch (severity) {
      case INFO:
        return Event.AlertType.INFO;
      case WARNING:
        return Event.AlertType.WARNING;
      case ERROR:
        return Event.AlertType.ERROR;
      case CRITICAL:
        return Event.AlertType.ERROR;
      default:
        return Event.AlertType.INFO;
    }
  }

  private ServiceCheck.Status mapHealthStatusToDatadog(ObservabilityEvent.HealthStatus status) {
    switch (status) {
      case HEALTHY:
        return ServiceCheck.Status.OK;
      case DEGRADED:
        return ServiceCheck.Status.WARNING;
      case UNHEALTHY:
        return ServiceCheck.Status.CRITICAL;
      case UNKNOWN:
        return ServiceCheck.Status.UNKNOWN;
      default:
        return ServiceCheck.Status.UNKNOWN;
    }
  }
}
