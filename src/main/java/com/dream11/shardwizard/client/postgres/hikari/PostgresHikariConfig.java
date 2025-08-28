package com.dream11.shardwizard.client.postgres.hikari;

import com.typesafe.config.Optional;
import com.zaxxer.hikari.HikariConfig;
import java.util.concurrent.TimeUnit;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;

@Data
@NoArgsConstructor
@Slf4j
public class PostgresHikariConfig {

  static final String DEFAULT_HOST = "127.0.0.1";
  static final int DEFAULT_PORT = 5432;
  static final String DEFAULT_POOL_NAME = "postgres-hikari-pool";
  static final boolean DEFAULT_USE_SERVER_CACHE_PREPARE_STATEMENT = false;
  static final boolean DEFAULT_USE_CACHE_PREPARE_STATEMENT = true;
  static final int DEFAULT_PREP_STATEMENT_CACHE_SIZE = 250;
  static final int DEFAULT_PREP_STATEMENT_CACHE_SQL_LIMIT = 2048;
  static final int DEFAULT_MAX_POOL_SIZE = 20; // Equal to the default number of vertx worker pool
  static final int DEFAULT_CONNECTION_TIMEOUT_MS = 500;
  static final String DEFAULT_DRIVER_CLASS_NAME = "org.postgresql.Driver";
  static final boolean DEFAULT_SSL = false;
  static final long DEFAULT_CONNECTION_MAX_LIFETIME_MS = TimeUnit.MINUTES.toMillis(30);
  static final long DEFAULT_IDLE_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(20);

  static final boolean REWRITE_BATCHED_STATEMENTS = false;
  @NonNull @Optional String masterHost = DEFAULT_HOST;
  @NonNull @Optional String slaveHost = DEFAULT_HOST;
  @Optional int port = DEFAULT_PORT;

  @NonNull String userName;
  @NonNull String password;
  @Optional boolean cachePrepStmts = DEFAULT_USE_CACHE_PREPARE_STATEMENT;
  @Optional boolean useServerPrepStmts = DEFAULT_USE_SERVER_CACHE_PREPARE_STATEMENT;
  @Optional int prepStmtCacheSize = DEFAULT_PREP_STATEMENT_CACHE_SIZE;
  @Optional int prepStmtCacheSqlLimit = DEFAULT_PREP_STATEMENT_CACHE_SQL_LIMIT;
  @Optional String poolName = DEFAULT_POOL_NAME;
  @Optional int maxPoolSize = DEFAULT_MAX_POOL_SIZE;
  @Optional int connectionTimeout = DEFAULT_CONNECTION_TIMEOUT_MS;
  @Optional String driverClassName = DEFAULT_DRIVER_CLASS_NAME;
  @NonNull String database;
  @Optional boolean useSSL = DEFAULT_SSL;
  @Optional long maxLifetimeMs = DEFAULT_CONNECTION_MAX_LIFETIME_MS;

  @Optional long idleTimeoutMs = DEFAULT_IDLE_TIMEOUT_MS;

  @Optional boolean rewriteBatchedStatements = REWRITE_BATCHED_STATEMENTS;

  private String getJDBCHost(String host) {
    return String.format("jdbc:postgresql://%s:%s/%s", host, this.port, this.database);
  }

  public HikariConfig getClientConfig(boolean isMaster) {
    HikariConfig hikariConfig = getHikariConfig(isMaster);
    hikariConfig.addDataSourceProperty("cachePrepStmts", this.cachePrepStmts);
    hikariConfig.addDataSourceProperty("useServerPrepStmts", this.useServerPrepStmts);
    hikariConfig.addDataSourceProperty("prepStmtCacheSize", this.prepStmtCacheSize);
    hikariConfig.addDataSourceProperty("prepStmtCacheSqlLimit", this.prepStmtCacheSqlLimit);
    hikariConfig.addDataSourceProperty("rewriteBatchedStatements", this.rewriteBatchedStatements);
    hikariConfig.addDataSourceProperty("useSSL", this.useSSL);
    hikariConfig.addDataSourceProperty(
        "useOldAliasMetadataBehavior", "true"); // to support AS in sql statement

    return hikariConfig;
  }

  private @NotNull HikariConfig getHikariConfig(boolean isMaster) {
    HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setMaxLifetime(this.maxLifetimeMs);
    hikariConfig.setIdleTimeout(this.idleTimeoutMs);
    hikariConfig.setJdbcUrl(isMaster ? getJDBCHost(this.masterHost) : getJDBCHost(this.slaveHost));
    hikariConfig.setUsername(this.getUserName());
    hikariConfig.setPassword(this.getPassword());
    hikariConfig.setConnectionTimeout(this.connectionTimeout);
    hikariConfig.setMaximumPoolSize(this.maxPoolSize);
    hikariConfig.setDriverClassName(this.driverClassName);
    hikariConfig.setPoolName(this.poolName);
    hikariConfig.setConnectionTestQuery("SELECT 1");
    return hikariConfig;
  }
}
