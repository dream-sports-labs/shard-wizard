package com.dream11.shardwizard.metric;

import static com.dream11.shardwizard.constant.Constants.DATADOG;
import static com.dream11.shardwizard.constant.Constants.NOOP;

import com.dream11.shardwizard.config.DatadogConfig;
import com.dream11.shardwizard.config.ObservabilityConfig;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.metric.impl.DatadogObservabilityAdapter;
import com.dream11.shardwizard.metric.impl.NoOpObservabilityAdapter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

// This is an enhanced Factory using Bill Pugh Singleton Pattern (Initialization-on-demand holder)
@Slf4j
public class ObservabilityServiceFactory {
  private static volatile ObservabilityConfig globalConfig;
  private static final Object configLock = new Object();
  // This is for Thread-safe registry for custom adapter factories
  private static final Map<String, AdapterFactory> customAdapterFactories =
      new ConcurrentHashMap<>();

  private ObservabilityServiceFactory() {
    // Private constructor to prevent instantiation
  }

  /**
   * Functional interface for creating custom observability adapters. This allows clients to provide
   * their own adapter creation logic.
   */
  @FunctionalInterface
  public interface AdapterFactory {
    /**
     * Creates an observability adapter instance.
     *
     * @param config The observability configuration
     * @return A new ObservabilityService instance
     * @throws Exception if adapter creation fails
     */
    ObservabilityService create(ObservabilityConfig config) throws Exception;
  }

  /**
   * Registers a custom adapter factory for a given provider type. This method is thread-safe and
   * can be called at any time before getInstance().
   *
   * @param providerType The provider type (e.g., "custom-metrics", "elasticsearch")
   * @param factory The factory function to create the adapter
   * @throws IllegalArgumentException if providerType is null/empty or factory is null
   * @throws IllegalStateException if trying to override built-in providers
   */
  public static void registerCustomAdapter(String providerType, AdapterFactory factory) {
    if (StringUtils.isBlank(providerType)) {
      throw new IllegalArgumentException("Provider type cannot be null or empty");
    }
    if (factory == null) {
      throw new IllegalArgumentException("Adapter factory cannot be null");
    }

    String normalizedType = providerType.toLowerCase().trim();

    // Prevent overriding built-in providers
    if (DATADOG.equals(normalizedType) || NOOP.equals(normalizedType)) {
      throw new IllegalStateException(
          "Cannot override built-in provider: "
              + normalizedType
              + ". Use a different provider name.");
    }

    AdapterFactory existing = customAdapterFactories.put(normalizedType, factory);
    if (existing != null) {
      log.warn("Overriding existing custom adapter factory for provider: {}", normalizedType);
    } else {
      log.info("Registered custom adapter factory for provider: {}", normalizedType);
    }
  }

  /**
   * Removes a custom adapter factory.
   *
   * @param providerType The provider type to remove
   * @return true if the factory was removed, false if it didn't exist
   */
  public static boolean unregisterCustomAdapter(String providerType) {
    if (StringUtils.isBlank(providerType)) {
      return false;
    }

    String normalizedType = providerType.toLowerCase().trim();
    boolean removed = customAdapterFactories.remove(normalizedType) != null;
    if (removed) {
      log.info("Unregistered custom adapter factory for provider: {}", normalizedType);
    }
    return removed;
  }

  /**
   * Gets all registered custom provider types.
   *
   * @return A copy of the set of registered custom provider types
   */
  public static Set<String> getRegisteredCustomProviders() {
    return new HashSet<>(customAdapterFactories.keySet());
  }

  /*
   * Bill Pugh Singleton implementation using static inner class.
   * This ensures thread-safe lazy initialization without synchronization overhead.
   * 	The JVM guarantees that a static class is only loaded and initialized once, and class loading is thread-safe as per the Java Language Specification.
   * The SingletonHolder class is loaded only when getInstance() is called for the first time.
   */
  private static class SingletonHolder {
    // This instance is created only when SingletonHolder class is loaded
    private static final ObservabilityService INSTANCE = createObservabilityService();

    /*
     * Creates the ObservabilityService instance based on the global configuration.
     * This method is called only once when the SingletonHolder class is loaded.
     */
    private static ObservabilityService createObservabilityService() {
      ObservabilityConfig config = globalConfig;

      if (Objects.isNull(config)) {
        log.warn("ObservabilityService configuration not set, using NoOp adapter");
        return new NoOpObservabilityAdapter();
      }

      String providerType =
          config.getProvider() != null ? config.getProvider().toLowerCase() : "noop";
      String serviceName = config.getServiceName() != null ? config.getServiceName() : "unknown";
      Map<String, String> defaultTags =
          config.getDefaultTags() != null ? config.getDefaultTags() : new HashMap<>();

      log.info(
          "Creating ObservabilityService of type: {} for service: {}", providerType, serviceName);

      try {
        switch (providerType) {
          case DATADOG:
            DatadogConfig ddConfig = config.getDatadogConfig();
            if (ddConfig == null) {
              log.error("Datadog configuration is missing, falling back to NoOp adapter");
              return new NoOpObservabilityAdapter();
            }
            return new DatadogObservabilityAdapter(
                serviceName,
                defaultTags,
                ddConfig.getHost(),
                ddConfig.getPort(),
                ddConfig.getPrefix());

            /* TODO: Need to add this when we have integration for other metrics
            case "newrelic":
                return new NewRelicObservabilityAdapter(serviceName, defaultTags);

            case "prometheus":
                PrometheusConfig promConfig = config.getPrometheusConfig();
                if (promConfig == null) {
                    log.error("Prometheus configuration is missing, falling back to NoOp adapter");
                    return new NoOpObservabilityAdapter();
                }
                return new PrometheusObservabilityAdapter(
                        serviceName,
                        defaultTags,
                        promConfig.getRegistry()
                );*/

          case NOOP:
            log.info("Using NoOp ObservabilityService as configured");
            return new NoOpObservabilityAdapter();

          default:
            log.warn("Unknown provider type: {}, using NoOp ObservabilityService", providerType);
            return new NoOpObservabilityAdapter();
        }
      } catch (Exception e) {
        log.error(
            "Failed to create ObservabilityService for provider: {}, falling back to NoOp adapter",
            providerType,
            e);
        return new NoOpObservabilityAdapter();
      }
    }
  }

  /*
   * Sets the global configuration for the ObservabilityService.
   * This method must be called before the first call to getInstance().
   *
   * @param config The configuration to use for creating the ObservabilityService
   * @throws IllegalStateException if getInstance() has already been called
   */

  public static void setConfiguration(ObservabilityConfig config) {
    synchronized (configLock) {
      if (isInstanceCreated()) {
        throw new IllegalStateException(
            "Configuration cannot be changed after ObservabilityService instance has been created. "
                + "Call setConfiguration() before first getInstance() call.");
      }
      globalConfig = config;
      log.info(
          "ObservabilityService configuration set for provider: {}",
          config != null ? config.getProvider() : "null");
    }
  }

  public static ObservabilityConfig createObservabilityConfig(ShardManagerConfig config) {
    String provider =
        StringUtils.isNotEmpty(config.getMetricsAgent())
            ? config.getMetricsAgent().toLowerCase()
            : "";
    DatadogConfig datadogConfig =
        Objects.isNull(config.getMetricsAgentConfig())
            ? null
            : config.getMetricsAgentConfig().getDatadog();

    return ObservabilityConfig.builder()
        .provider(provider)
        .datadogConfig(datadogConfig)
        .serviceName(config.getServiceName())
        .build();
  }

  /*
   * Gets the singleton instance of ObservabilityService.
   * This method is thread-safe and uses lazy initialization.
   *
   * @return The singleton ObservabilityService instance
   */
  public static ObservabilityService getInstance() {
    // The Bill Pugh pattern ensures this is thread-safe without synchronization
    return SingletonHolder.INSTANCE;
  }

  /*
   * Checks if the singleton instance has been created.
   * This is useful for configuration validation.
   *
   * @return true if the instance has been created, false otherwise
   */
  private static boolean isInstanceCreated() {
    try {
      // Check if the SingletonHolder class has been loaded
      // This is a bit hacky but works for our use case
      Class<?> holderClass =
          Class.forName(
              ObservabilityServiceFactory.class.getName() + "$SingletonHolder",
              false,
              ObservabilityServiceFactory.class.getClassLoader());
      return holderClass != null;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Gets the current configuration. This method is primarily for testing and debugging purposes.
   *
   * @return The current ObservabilityConfig or null if not set
   */
  public static ObservabilityConfig getConfiguration() {
    synchronized (configLock) {
      return globalConfig;
    }
  }

  /**
   * Resets the factory state. This method is primarily for testing purposes. Note: This method
   * cannot reset an already created singleton instance due to the nature of the Bill Pugh pattern.
   * The JVM would need to be restarted for a complete reset.
   *
   * @deprecated This method is provided for testing compatibility but has limitations
   */
  @Deprecated
  public static void reset() {
    synchronized (configLock) {
      globalConfig = null;
      log.warn(
          "ObservabilityServiceFactory configuration reset. "
              + "Note: Already created singleton instance cannot be reset without JVM restart.");
    }
  }
}
