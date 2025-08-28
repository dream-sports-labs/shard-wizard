package com.dream11.shardwizard.example.runs.metric.agent;

import static org.junit.Assert.*;

import com.dream11.shardwizard.config.DatadogConfig;
import com.dream11.shardwizard.config.MetricAgentConfig;
import com.dream11.shardwizard.config.ObservabilityConfig;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.metric.ObservabilityService;
import com.dream11.shardwizard.metric.ObservabilityServiceFactory;
import com.dream11.shardwizard.metric.impl.NoOpObservabilityAdapter;
import java.util.Set;
import org.junit.Test;

public class ObservabilityServiceFactoryTest {

  @Test
  public void testGetInstance_WithNoConfiguration_ShouldReturnNoOpAdapter() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // When
    ObservabilityService service = ObservabilityServiceFactory.getInstance();

    // Then
    assertNotNull(service);
    assertTrue(service instanceof NoOpObservabilityAdapter);
  }

  @Test(expected = IllegalStateException.class)
  public void testSetConfiguration_AfterGetInstance_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityConfig config1 =
        ObservabilityConfig.builder().provider("noop").serviceName("test-service").build();
    ObservabilityServiceFactory.setConfiguration(config1);

    // When - get instance first
    ObservabilityServiceFactory.getInstance();

    // Then - try to set configuration again should throw exception
    ObservabilityConfig config2 =
        ObservabilityConfig.builder().provider("datadog").serviceName("test-service-2").build();
    ObservabilityServiceFactory.setConfiguration(config2);
  }

  @Test
  public void testGetConfiguration_WithNoConfig_ShouldReturnNull() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // When
    ObservabilityConfig config = ObservabilityServiceFactory.getConfiguration();

    // Then
    assertNull(config);
  }

  @Test
  public void testCreateObservabilityConfig_WithValidShardManagerConfig() {
    // Given
    ShardManagerConfig shardManagerConfig = new ShardManagerConfig();
    shardManagerConfig.setMetricsAgent("datadog");
    shardManagerConfig.setServiceName("test-service");

    DatadogConfig datadogConfig = new DatadogConfig();
    datadogConfig.setHost("localhost");
    datadogConfig.setPort(8125);
    datadogConfig.setPrefix("test");

    MetricAgentConfig metricAgentConfig = new MetricAgentConfig();
    metricAgentConfig.setDatadog(datadogConfig);
    shardManagerConfig.setMetricsAgentConfig(metricAgentConfig);

    // When
    ObservabilityConfig config =
        ObservabilityServiceFactory.createObservabilityConfig(shardManagerConfig);

    // Then
    assertNotNull(config);
    assertEquals("datadog", config.getProvider());
    assertEquals("test-service", config.getServiceName());
    assertNotNull(config.getDatadogConfig());
    assertEquals("localhost", config.getDatadogConfig().getHost());
    assertEquals(8125, config.getDatadogConfig().getPort());
    assertEquals("test", config.getDatadogConfig().getPrefix());
  }

  @Test
  public void testCreateObservabilityConfig_WithNullMetricsAgent_ShouldUseEmptyString() {
    // Given
    ShardManagerConfig shardManagerConfig = new ShardManagerConfig();
    shardManagerConfig.setMetricsAgent(null);
    shardManagerConfig.setServiceName("test-service");

    // When
    ObservabilityConfig config =
        ObservabilityServiceFactory.createObservabilityConfig(shardManagerConfig);

    // Then
    assertNotNull(config);
    assertEquals("", config.getProvider());
    assertEquals("test-service", config.getServiceName());
  }

  @Test
  public void testCreateObservabilityConfig_WithEmptyMetricsAgent_ShouldUseEmptyString() {
    // Given
    ShardManagerConfig shardManagerConfig = new ShardManagerConfig();
    shardManagerConfig.setMetricsAgent("");
    shardManagerConfig.setServiceName("test-service");

    // When
    ObservabilityConfig config =
        ObservabilityServiceFactory.createObservabilityConfig(shardManagerConfig);

    // Then
    assertNotNull(config);
    assertEquals("", config.getProvider());
    assertEquals("test-service", config.getServiceName());
  }

  @Test
  public void testCreateObservabilityConfig_WithNullDatadogConfig_ShouldWorkCorrectly() {
    // Given
    ShardManagerConfig shardManagerConfig = new ShardManagerConfig();
    shardManagerConfig.setMetricsAgent("datadog");
    shardManagerConfig.setServiceName("test-service");

    MetricAgentConfig metricAgentConfig = new MetricAgentConfig();
    metricAgentConfig.setDatadog(null);
    shardManagerConfig.setMetricsAgentConfig(metricAgentConfig);

    // When
    ObservabilityConfig config =
        ObservabilityServiceFactory.createObservabilityConfig(shardManagerConfig);

    // Then
    assertNotNull(config);
    assertEquals("datadog", config.getProvider());
    assertEquals("test-service", config.getServiceName());
    assertNull(config.getDatadogConfig());
  }

  @Test
  public void testCreateObservabilityConfig_WithNullShardManagerConfig_ShouldThrowException() {
    // When & Then
    try {
      ObservabilityServiceFactory.createObservabilityConfig(null);
      fail("Should throw NullPointerException");
    } catch (NullPointerException e) {
      // Expected exception
    }
  }

  @Test
  public void testCreateObservabilityConfig_WithNullMetricAgentConfig_ShouldWorkCorrectly() {
    // Given
    ShardManagerConfig shardManagerConfig = new ShardManagerConfig();
    shardManagerConfig.setMetricsAgent("datadog");
    shardManagerConfig.setServiceName("test-service");
    shardManagerConfig.setMetricsAgentConfig(null);

    // When
    ObservabilityConfig config =
        ObservabilityServiceFactory.createObservabilityConfig(shardManagerConfig);

    // Then
    assertNotNull(config);
    assertEquals("datadog", config.getProvider());
    assertEquals("test-service", config.getServiceName());
    assertNull(config.getDatadogConfig());
  }

  // ==================== AdapterFactory Interface Tests ====================

  @Test
  public void testAdapterFactory_WithValidImplementation() throws Exception {
    // Given
    ObservabilityServiceFactory.AdapterFactory factory =
        config -> {
          return new NoOpObservabilityAdapter();
        };

    // When
    ObservabilityService service = factory.create(null);

    // Then
    assertNotNull(service);
    assertTrue(service instanceof NoOpObservabilityAdapter);
  }

  @Test
  public void testAdapterFactory_WithConfigurationParameter() throws Exception {
    // Given
    ObservabilityConfig testConfig =
        ObservabilityConfig.builder().provider("custom").serviceName("test-service").build();

    ObservabilityServiceFactory.AdapterFactory factory =
        config -> {
          assertNotNull(config);
          assertEquals("custom", config.getProvider());
          assertEquals("test-service", config.getServiceName());
          return new NoOpObservabilityAdapter();
        };

    // When
    ObservabilityService service = factory.create(testConfig);

    // Then
    assertNotNull(service);
    assertTrue(service instanceof NoOpObservabilityAdapter);
  }

  @Test
  public void testAdapterFactory_WithExceptionHandling() {
    // Given
    ObservabilityServiceFactory.AdapterFactory factory =
        config -> {
          throw new RuntimeException("Test exception");
        };

    // When & Then
    try {
      factory.create(null);
      fail("Should throw RuntimeException");
    } catch (Exception e) {
      assertEquals("Test exception", e.getMessage());
    }
  }

  // ==================== Custom Adapter Registration Tests ====================

  @Test
  public void testRegisterCustomAdapter_WithValidProvider() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "custom-metrics";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);

    // Then
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();
    assertTrue(registeredProviders.contains(providerType.toLowerCase()));
  }

  @Test
  public void testRegisterCustomAdapter_WithCaseInsensitiveProvider() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "CUSTOM-METRICS";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);

    // Then
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();
    assertTrue(registeredProviders.contains("custom-metrics"));
  }

  @Test
  public void testRegisterCustomAdapter_WithWhitespaceProvider() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "  custom-metrics  ";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);

    // Then
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();
    assertTrue(registeredProviders.contains("custom-metrics"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterCustomAdapter_WithNullProvider_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter(null, factory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterCustomAdapter_WithEmptyProvider_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter("", factory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterCustomAdapter_WithBlankProvider_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter("   ", factory);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRegisterCustomAdapter_WithNullFactory_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "custom-metrics";

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter(providerType, null);
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterCustomAdapter_WithBuiltInProvider_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "datadog";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);
  }

  @Test(expected = IllegalStateException.class)
  public void testRegisterCustomAdapter_WithNoOpProvider_ShouldThrowException() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "noop";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();

    // When & Then
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);
  }

  @Test
  public void testUnregisterCustomAdapter_WithValidProvider_ShouldReturnTrue() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "custom-metrics";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter(providerType);

    // Then
    assertTrue(result);
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();
    assertFalse(registeredProviders.contains("custom-metrics"));
  }

  @Test
  public void testUnregisterCustomAdapter_WithCaseInsensitiveProvider_ShouldReturnTrue() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "custom-metrics";
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();
    ObservabilityServiceFactory.registerCustomAdapter(providerType, factory);

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter("CUSTOM-METRICS");

    // Then
    assertTrue(result);
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();
    assertFalse(registeredProviders.contains("custom-metrics"));
  }

  @Test
  public void testUnregisterCustomAdapter_WithNonExistentProvider_ShouldReturnFalse() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "non-existent";

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter(providerType);

    // Then
    assertFalse(result);
  }

  @Test
  public void testUnregisterCustomAdapter_WithNullProvider_ShouldReturnFalse() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = null;

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter(providerType);

    // Then
    assertFalse(result);
  }

  @Test
  public void testUnregisterCustomAdapter_WithEmptyProvider_ShouldReturnFalse() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "";

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter(providerType);

    // Then
    assertFalse(result);
  }

  @Test
  public void testUnregisterCustomAdapter_WithBlankProvider_ShouldReturnFalse() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    String providerType = "   ";

    // When
    boolean result = ObservabilityServiceFactory.unregisterCustomAdapter(providerType);

    // Then
    assertFalse(result);
  }

  @Test
  public void testGetRegisteredCustomProviders_WithMultipleProviders_ShouldReturnAllProviders() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();
    ObservabilityServiceFactory.registerCustomAdapter("provider1", factory);
    ObservabilityServiceFactory.registerCustomAdapter("provider2", factory);
    ObservabilityServiceFactory.registerCustomAdapter("provider3", factory);

    // When
    Set<String> registeredProviders = ObservabilityServiceFactory.getRegisteredCustomProviders();

    // Then
    assertNotNull(registeredProviders);
    assertEquals(3, registeredProviders.size());
    assertTrue(registeredProviders.contains("provider1"));
    assertTrue(registeredProviders.contains("provider2"));
    assertTrue(registeredProviders.contains("provider3"));
  }

  @Test
  public void testGetRegisteredCustomProviders_ReturnsCopyOfSet() {
    // Reset before test
    ObservabilityServiceFactory.reset();

    // Given
    ObservabilityServiceFactory.AdapterFactory factory = config -> new NoOpObservabilityAdapter();
    ObservabilityServiceFactory.registerCustomAdapter("provider1", factory);

    // When
    Set<String> registeredProviders1 = ObservabilityServiceFactory.getRegisteredCustomProviders();
    Set<String> registeredProviders2 = ObservabilityServiceFactory.getRegisteredCustomProviders();

    // Then
    assertNotNull(registeredProviders1);
    assertNotNull(registeredProviders2);
    assertNotSame(registeredProviders1, registeredProviders2); // Should be different objects
    assertEquals(registeredProviders1, registeredProviders2); // But same content
  }
}
