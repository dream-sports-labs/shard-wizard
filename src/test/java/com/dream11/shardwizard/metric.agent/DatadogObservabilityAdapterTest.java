package com.dream11.shardwizard.metric.agent;

import static org.junit.Assert.*;

import com.dream11.shardwizard.dto.ObservabilityEvent;
import com.dream11.shardwizard.metric.impl.DatadogObservabilityAdapter;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class DatadogObservabilityAdapterTest {

  private DatadogObservabilityAdapter adapter;
  private static final String SERVICE_NAME = "test-service";
  private static final String HOST = "localhost";
  private static final int PORT = 8125;
  private static final String PREFIX = "test";

  @Before
  public void setUp() {
    Map<String, String> defaultTags = new HashMap<>();
    defaultTags.put("environment", "test");
    defaultTags.put("version", "1.0.0");

    adapter = new DatadogObservabilityAdapter(SERVICE_NAME, defaultTags, HOST, PORT, PREFIX);
  }

  @Test
  public void testConstructor_WithValidParameters() {
    Map<String, String> defaultTags = new HashMap<>();
    defaultTags.put("environment", "test");
    defaultTags.put("version", "1.0.0");

    DatadogObservabilityAdapter testAdapter =
        new DatadogObservabilityAdapter(SERVICE_NAME, defaultTags, HOST, PORT, PREFIX);
    assertNotNull(testAdapter);
  }

  @Test
  public void testRecordMetric_WithValidParameters() {
    String metricName = "test.metric";
    double value = 42.5;
    Map<String, String> tags = new HashMap<>();
    tags.put("tag1", "value1");
    tags.put("tag2", "value2");

    // When & Then - should not throw exception
    try {
      adapter.recordMetric(metricName, value, tags);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordMetric should not throw exception: " + e.getMessage());
    }
  }

  @Test
  public void testRecordMetric_WithNullTags() {
    // Given
    String metricName = "test.metric";
    double value = 42.5;

    // When & Then - should not throw exception
    try {
      adapter.recordMetric(metricName, value, null);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordMetric should not throw exception with null tags: " + e.getMessage());
    }
  }

  @Test
  public void testRecordEvent_WithValidEvent() {
    // Given
    ObservabilityEvent event =
        ObservabilityEvent.builder()
            .title("Test Event")
            .description("Test event description")
            .severity(ObservabilityEvent.EventSeverity.ERROR)
            .tags(Map.of("event_tag", "event_value"))
            .build();

    // When & Then - should not throw exception
    try {
      adapter.recordEvent(event);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordEvent should not throw exception: " + e.getMessage());
    }
  }

  @Test
  public void testRecordEvent_WithNullTags() {
    // Given
    ObservabilityEvent event =
        ObservabilityEvent.builder()
            .title("Test Event")
            .description("Test event description")
            .severity(ObservabilityEvent.EventSeverity.INFO)
            .build();

    // When & Then - should not throw exception
    try {
      adapter.recordEvent(event);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordEvent should not throw exception with null tags: " + e.getMessage());
    }
  }

  @Test
  public void testRecordServiceHealth_WithValidParameters() {
    // Given
    String serviceName = "test-service";
    ObservabilityEvent.HealthStatus status = ObservabilityEvent.HealthStatus.HEALTHY;
    Map<String, String> metadata = new HashMap<>();
    metadata.put("message", "Service is healthy");
    metadata.put("check_time", "2025-01-01T00:00:00Z");

    // When & Then - should not throw exception
    try {
      adapter.recordServiceHealth(serviceName, status, metadata);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordServiceHealth should not throw exception: " + e.getMessage());
    }
  }

  @Test
  public void testRecordServiceHealth_WithNullMetadata() {
    // Given
    String serviceName = "test-service";
    ObservabilityEvent.HealthStatus status = ObservabilityEvent.HealthStatus.DEGRADED;

    // When & Then - should not throw exception
    try {
      adapter.recordServiceHealth(serviceName, status, null);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordServiceHealth should not throw exception with null metadata: " + e.getMessage());
    }
  }

  @Test
  public void testRecordTrace_WithValidParameters() {
    // Given
    String operationName = "test-operation";
    long durationMs = 150;
    Map<String, String> context = new HashMap<>();
    context.put("user_id", "12345");
    context.put("request_id", "req-123");

    // When & Then - should not throw exception
    try {
      adapter.recordTrace(operationName, durationMs, context);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordTrace should not throw exception: " + e.getMessage());
    }
  }

  @Test
  public void testRecordTrace_WithNullContext() {
    // Given
    String operationName = "test-operation";
    long durationMs = 200;

    // When & Then - should not throw exception
    try {
      adapter.recordTrace(operationName, durationMs, null);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordTrace should not throw exception with null context: " + e.getMessage());
    }
  }

  @Test
  public void testRecordTrace_WithZeroDuration() {
    // Given
    String operationName = "test-operation";
    long durationMs = 0;

    // When & Then - should not throw exception
    try {
      adapter.recordTrace(operationName, durationMs, new HashMap<>());
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordTrace should not throw exception with zero duration: " + e.getMessage());
    }
  }

  @Test
  public void testRecordTrace_WithNegativeDuration() {
    // Given
    String operationName = "test-operation";
    long durationMs = -100;

    // When & Then - should not throw exception
    try {
      adapter.recordTrace(operationName, durationMs, new HashMap<>());
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordTrace should not throw exception with negative duration: " + e.getMessage());
    }
  }

  @Test
  public void testRecordEvent_WithAllSeverityLevels() {
    // Test all severity levels
    ObservabilityEvent.EventSeverity[] severities = {
      ObservabilityEvent.EventSeverity.INFO,
      ObservabilityEvent.EventSeverity.WARNING,
      ObservabilityEvent.EventSeverity.ERROR,
      ObservabilityEvent.EventSeverity.CRITICAL
    };

    for (ObservabilityEvent.EventSeverity severity : severities) {
      // Given
      ObservabilityEvent event =
          ObservabilityEvent.builder()
              .title("Test Event - " + severity)
              .description("Test event description")
              .severity(severity)
              .build();

      // When & Then - should not throw exception
      try {
        adapter.recordEvent(event);
        // Test passes if no exception is thrown
      } catch (Exception e) {
        fail(
            "recordEvent should not throw exception for severity "
                + severity
                + ": "
                + e.getMessage());
      }
    }
  }

  @Test
  public void testRecordServiceHealth_WithAllHealthStatuses() {
    // Test all health statuses
    ObservabilityEvent.HealthStatus[] statuses = {
      ObservabilityEvent.HealthStatus.HEALTHY,
      ObservabilityEvent.HealthStatus.DEGRADED,
      ObservabilityEvent.HealthStatus.UNHEALTHY,
      ObservabilityEvent.HealthStatus.UNKNOWN
    };

    for (ObservabilityEvent.HealthStatus status : statuses) {
      // Given
      String serviceName = "test-service-" + status;
      Map<String, String> metadata = new HashMap<>();
      metadata.put("status", status.toString());

      // When & Then - should not throw exception
      try {
        adapter.recordServiceHealth(serviceName, status, metadata);
        // Test passes if no exception is thrown
      } catch (Exception e) {
        fail(
            "recordServiceHealth should not throw exception for status "
                + status
                + ": "
                + e.getMessage());
      }
    }
  }

  @Test
  public void testRecordMetric_WithSpecialCharacters() {
    // Given
    String metricName = "test.metric.with.special.chars";
    double value = 42.5;
    Map<String, String> tags = new HashMap<>();
    tags.put("tag_with_special_chars", "value_with_special_chars");
    tags.put("tag_with.dots", "value.with.dots");

    // When & Then - should not throw exception
    try {
      adapter.recordMetric(metricName, value, tags);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordMetric should not throw exception with special characters: " + e.getMessage());
    }
  }

  @Test
  public void testRecordEvent_WithEmptyStrings() {
    // Given
    ObservabilityEvent event =
        ObservabilityEvent.builder()
            .title("")
            .description("")
            .severity(ObservabilityEvent.EventSeverity.INFO)
            .build();

    // When & Then - should not throw exception
    try {
      adapter.recordEvent(event);
      // Test passes if no exception is thrown
    } catch (Exception e) {
      fail("recordEvent should not throw exception with empty strings: " + e.getMessage());
    }
  }
}
