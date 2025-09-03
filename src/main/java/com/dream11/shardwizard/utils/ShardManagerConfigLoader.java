package com.dream11.shardwizard.utils;

import static com.dream11.shardwizard.constant.Constants.SHARD_MANAGER_CONFIG_FOLDER;

import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.constant.DatabaseType;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

/**
 * Centralized utility for loading ShardManagerConfig with intelligent fallback mechanism.
 *
 * <p>Fallback order: 1. Database-specific config (e.g., postgres.conf, dynamo.conf, s3.conf) 2.
 * default.conf (universal fallback with all database configurations) 3. Hard-coded
 * DatabaseType.POSTGRES (ultimate safety net)
 */
@Slf4j
@UtilityClass
public class ShardManagerConfigLoader {

  private static final String DEFAULT_CONFIG_NAME = "default";

  /**
   * Loads ShardManagerConfig for the given DatabaseType with intelligent fallback.
   *
   * @param sourceType The database type to load config for
   * @return ShardManagerConfig loaded with fallback mechanism
   */
  public static ShardManagerConfig loadConfigWithFallback(DatabaseType sourceType) {
    String specificConfigName = sourceType.name().toLowerCase();
    String specificConfigPath =
        "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/" + specificConfigName + ".conf";
    String defaultConfigPath =
        "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/" + DEFAULT_CONFIG_NAME + ".conf";

    try {
      // Try the database-specific config first
      log.debug("Attempting to load specific config from: {}", specificConfigPath);
      return ConfigUtils.fromConfigFile(specificConfigPath, ShardManagerConfig.class);
    } catch (Exception e) {
      log.warn(
          "Failed to load specific config from {}, falling back to default config",
          specificConfigPath);
      try {
        // Fallback to default.conf
        log.debug("Attempting to load default config from: {}", defaultConfigPath);
        return ConfigUtils.fromConfigFile(defaultConfigPath, ShardManagerConfig.class);
      } catch (Exception ex) {
        log.error("Failed to load both specific and default configurations", ex);
        throw new RuntimeException(
            "Unable to load shard manager configuration for " + sourceType, ex);
      }
    }
  }

  /**
   * Determines the DatabaseType to use with intelligent fallback.
   *
   * <p>Fallback order: 1. Explicit DatabaseType parameter (if provided) 2. System property
   * "shard.source.type" 3. sourceType from default.conf 4. Hard-coded DatabaseType.POSTGRES
   *
   * @param explicitType Explicitly provided DatabaseType (can be null)
   * @return DatabaseType to use
   */
  public static DatabaseType resolveDatabaseType(DatabaseType explicitType) {
    // If explicitly provided, use that
    if (explicitType != null) {
      log.debug("Using explicitly provided DatabaseType: {}", explicitType);
      return explicitType;
    }

    // Try system property first
    String sourceTypeProperty = System.getProperty("shard.source.type");
    if (sourceTypeProperty != null) {
      try {
        DatabaseType typeFromProperty = DatabaseType.valueOf(sourceTypeProperty.toUpperCase());
        log.debug("Using DatabaseType from system property: {}", typeFromProperty);
        return typeFromProperty;
      } catch (IllegalArgumentException e) {
        log.warn(
            "Invalid DatabaseType in system property 'shard.source.type': {}", sourceTypeProperty);
        // Fall through to default.conf fallback
      }
    }

    // Fallback to reading from default.conf
    try {
      ShardManagerConfig defaultConfig =
          ConfigUtils.fromConfigFile(
              "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/" + DEFAULT_CONFIG_NAME + ".conf",
              ShardManagerConfig.class);
      DatabaseType typeFromConfig = defaultConfig.getSourceType();
      log.debug("Using DatabaseType from default.conf: {}", typeFromConfig);
      return typeFromConfig;
    } catch (Exception e) {
      log.warn("Failed to read DatabaseType from default.conf, using hard-coded fallback", e);
      // Ultimate fallback
      return DatabaseType.POSTGRES;
    }
  }
}
