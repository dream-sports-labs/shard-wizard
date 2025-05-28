package com.dream11.shardwizard.dao.impl.postgresvertx;

import com.dream11.shardwizard.client.impl.postgres.vertx.PostgresVertxClient;
import com.dream11.shardwizard.client.impl.postgres.vertx.PostgresVertxClientImpl;
import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.sqlclient.PoolOptions;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class PostgresBaseDao extends BaseDaoAbstract {

  protected final Vertx vertx;
  private final ShardDetails shardDetails;
  protected PostgresVertxClient postgresVertxClient;

  protected PostgresBaseDao(Vertx vertx, ShardDetails shardDetails) {

    this.shardDetails = shardDetails;

    ShardConnectionParameters connectionParams =
        shardDetails.getShardConfig().getShardConnectionParams();

    this.vertx = vertx;
    PgConnectOptions writerConnectOptions =
        new PgConnectOptions()
            .setPort(connectionParams.getPort())
            .setHost(connectionParams.getWriterHost())
            .setDatabase(connectionParams.getDatabase())
            .setUser(connectionParams.getUsername())
            .setPassword(connectionParams.getPassword());

    PgConnectOptions readerConnectOptions =
        new PgConnectOptions()
            .setPort(connectionParams.getPort())
            .setHost(connectionParams.getReaderHost())
            .setDatabase(connectionParams.getDatabase())
            .setUser(connectionParams.getUsername())
            .setPassword(connectionParams.getPassword());

    PoolOptions poolOptions =
        new PoolOptions()
            .setMaxWaitQueueSize(connectionParams.getMaxWaitQueueSize())
            .setMaxSize(connectionParams.getMaxConnections());

    this.postgresVertxClient =
        new PostgresVertxClientImpl(vertx, readerConnectOptions, writerConnectOptions, poolOptions);
  }

  @Override
  public ShardDetails getShardDetails() {
    return shardDetails;
  }

  public Completable rxConnect() {
    return postgresVertxClient.rxConnect();
  }

  public Completable rxCloseConnections() {
    return postgresVertxClient.rxClose();
  }
}
