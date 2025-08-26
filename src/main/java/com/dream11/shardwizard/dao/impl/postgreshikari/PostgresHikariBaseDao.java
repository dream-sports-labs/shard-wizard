package com.dream11.shardwizard.dao.impl.postgreshikari;

import com.dream11.shardwizard.client.PostgresHikariClient;
import com.dream11.shardwizard.client.impl.PostgresHikariClientImpl;
import com.dream11.shardwizard.config.PostgresHikariConfig;
import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.vertx.reactivex.core.Vertx;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class PostgresHikariBaseDao extends BaseDaoAbstract {

  protected final PostgresHikariClient postgresHikariClient;
  protected final Vertx vertx;
  private final ShardDetails shardDetails;

  protected PostgresHikariBaseDao(Vertx vertx, ShardDetails shardDetails) {
    this.vertx = vertx;
    this.shardDetails = shardDetails;

    PostgresHikariConfig postgresConfig = new PostgresHikariConfig();
    ShardConnectionParameters connectionParams =
        shardDetails.getShardConfig().getShardConnectionParams();
    postgresConfig.setMasterHost(connectionParams.getWriterHost());
    postgresConfig.setSlaveHost(connectionParams.getReaderHost());
    postgresConfig.setPort(connectionParams.getPort());
    postgresConfig.setDatabase(connectionParams.getDatabase());
    postgresConfig.setUserName(connectionParams.getUsername());
    postgresConfig.setPassword(connectionParams.getPassword());
    postgresConfig.setConnectionTimeout(connectionParams.getConnectionTimeoutMs());
    postgresConfig.setCachePrepStmts(false);

    Optional.ofNullable(connectionParams.getMaxConnections())
        .ifPresent(postgresConfig::setMaxPoolSize);
    this.postgresHikariClient = new PostgresHikariClientImpl(vertx, postgresConfig, shardDetails);
  }

  @Override
  public ShardDetails getShardDetails() {
    return shardDetails;
  }

  @Override
  public Completable rxConnect() {
    return postgresHikariClient.rxConnect().andThen(Completable.defer(this::bootstrapConnections));
  }

  private Completable bootstrapConnections() {
    Completable writerHealthCompletable =
        postgresHikariClient.rxReadDBCall(RdsCluster.WRITER, "SELECT 1", List.of()).ignoreElement();
    Completable readerHealthCompletable =
        postgresHikariClient.rxReadDBCall(RdsCluster.READER, "SELECT 1", List.of()).ignoreElement();
    return Completable.mergeArray(writerHealthCompletable, readerHealthCompletable);
  }

  public Completable rxCloseConnections() {
    return postgresHikariClient.rxClose();
  }
}
