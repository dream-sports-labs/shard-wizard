package com.dream11.shardwizard.dao.impl.mysqlhikari;

import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.reactivex.Completable;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.Vertx;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class MySqlHikariBaseDao extends BaseDaoAbstract {

  // Default configuration constants
  private static final String DEFAULT_POOL_NAME = "mysql-hikari-pool";
  private static final boolean DEFAULT_USE_SERVER_CACHE_PREPARE_STATEMENT = false;
  private static final boolean DEFAULT_USE_CACHE_PREPARE_STATEMENT = true;
  private static final int DEFAULT_PREP_STATEMENT_CACHE_SIZE = 250;
  private static final int DEFAULT_PREP_STATEMENT_CACHE_SQL_LIMIT = 2048;
  private static final int DEFAULT_MAX_POOL_SIZE = 20;
  private static final int DEFAULT_CONNECTION_TIMEOUT = 10000;
  private static final String DEFAULT_DRIVER_CLASS_NAME = "com.mysql.cj.jdbc.Driver";
  private static final boolean DEFAULT_SSL = false;
  private static final boolean REWRITE_BATCHED_STATEMENTS = false;

  protected HikariDataSource masterDataSource;
  protected HikariDataSource slaveDataSource;
  protected final Vertx vertx;
  private final ShardDetails shardDetails;
  private final ShardConnectionParameters connectionParams;

  protected MySqlHikariBaseDao(Vertx vertx, ShardDetails shardDetails) {
    this.vertx = vertx;
    this.shardDetails = shardDetails;
    this.connectionParams = shardDetails.getShardConfig().getShardConnectionParams();
  }

  /** Creates HikariConfig for master (write) or slave (read) connection */
  private HikariConfig getClientConfig(boolean isMaster) {
    HikariConfig config = new HikariConfig();

    // Build JDBC URL for MySQL
    String host = isMaster ? connectionParams.getWriterHost() : connectionParams.getReaderHost();
    String jdbcUrl =
        String.format(
            "jdbc:mysql://%s:%s/%s?autoReconnect=true",
            host, connectionParams.getPort(), connectionParams.getDatabase());

    config.setJdbcUrl(jdbcUrl);
    config.setUsername(connectionParams.getUsername());
    config.setPassword(connectionParams.getPassword());

    config.setConnectionTimeout(DEFAULT_CONNECTION_TIMEOUT);
    config.setDriverClassName(DEFAULT_DRIVER_CLASS_NAME);
    config.setPoolName(isMaster ? DEFAULT_POOL_NAME + "-master" : DEFAULT_POOL_NAME + "-slave");

    // Set max pool size from connection params or use default
    Optional.ofNullable(connectionParams.getMaxConnections()).ifPresent(config::setMaximumPoolSize);
    if (connectionParams.getMaxConnections() == null) {
      config.setMaximumPoolSize(DEFAULT_MAX_POOL_SIZE);
    }

    config.addDataSourceProperty("cachePrepStmts", DEFAULT_USE_CACHE_PREPARE_STATEMENT);
    config.addDataSourceProperty("useServerPrepStmts", DEFAULT_USE_SERVER_CACHE_PREPARE_STATEMENT);
    config.addDataSourceProperty("prepStmtCacheSize", DEFAULT_PREP_STATEMENT_CACHE_SIZE);
    config.addDataSourceProperty("prepStmtCacheSqlLimit", DEFAULT_PREP_STATEMENT_CACHE_SQL_LIMIT);
    config.addDataSourceProperty("rewriteBatchedStatements", REWRITE_BATCHED_STATEMENTS);
    config.addDataSourceProperty("useSSL", DEFAULT_SSL);
    config.addDataSourceProperty(
        "useOldAliasMetadataBehavior", "true"); // to support AS in sql statement

    return config;
  }

  public Completable rxConnect() {
    return CompletableHelper.toCompletable(
        handler ->
            this.vertx.executeBlocking(
                promise -> {
                  try {
                    this.masterDataSource = new HikariDataSource(getClientConfig(true));
                    this.slaveDataSource = new HikariDataSource(getClientConfig(false));
                    log.info("Successfully created master and slave HikariCP connections");
                    promise.complete();
                  } catch (Exception e) {
                    log.error("Failed to create HikariCP data sources", e);
                    promise.fail(e);
                  }
                },
                handler));
  }

  public Completable rxCloseConnections() {
    return CompletableHelper.toCompletable(
        handler ->
            this.vertx.executeBlocking(
                promise -> {
                  try {
                    if (this.masterDataSource != null) {
                      this.masterDataSource.close();
                    }
                    if (this.slaveDataSource != null) {
                      this.slaveDataSource.close();
                    }
                    log.info("Successfully closed master and slave HikariCP connections");
                    promise.complete();
                  } catch (Exception e) {
                    log.error("Failed to close HikariCP connections", e);
                    promise.fail(e);
                  }
                },
                handler));
  }

  @Override
  public ShardDetails getShardDetails() {
    return shardDetails;
  }
}
