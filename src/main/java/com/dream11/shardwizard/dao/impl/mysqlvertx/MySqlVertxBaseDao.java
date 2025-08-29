package com.dream11.shardwizard.dao.impl.mysqlvertx;

import com.dream11.shardwizard.client.mysql.MysqlVertxClient;
import com.dream11.shardwizard.client.mysql.MysqlVertxClientImpl;
import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.sqlclient.PoolOptions;

public abstract class MySqlVertxBaseDao extends BaseDaoAbstract {

  protected final MysqlVertxClient mysqlVertxClient;
  protected final Vertx vertx;
  private final ShardDetails shardDetails;

  protected MySqlVertxBaseDao(Vertx vertx, ShardDetails shardDetails) {
    this.vertx = vertx;
    this.shardDetails = shardDetails;

    ShardConnectionParameters connectionParams =
        shardDetails.getShardConfig().getShardConnectionParams();

    MySQLConnectOptions masterOptions =
        new MySQLConnectOptions()
            .setHost(connectionParams.getWriterHost())
            .setPort(connectionParams.getPort())
            .setDatabase(connectionParams.getDatabase())
            .setUser(connectionParams.getUsername())
            .setPassword(connectionParams.getPassword());

    MySQLConnectOptions slaveOptions =
        new MySQLConnectOptions()
            .setHost(connectionParams.getReaderHost())
            .setPort(connectionParams.getPort())
            .setDatabase(connectionParams.getDatabase())
            .setUser(connectionParams.getUsername())
            .setPassword(connectionParams.getPassword());

    PoolOptions poolOptions =
        new PoolOptions()
            .setMaxSize(connectionParams.getMaxConnections())
            .setMaxWaitQueueSize(connectionParams.getMaxWaitQueueSize());

    this.mysqlVertxClient =
        new MysqlVertxClientImpl(vertx, masterOptions, slaveOptions, poolOptions, shardDetails);
  }

  @Override
  public ShardDetails getShardDetails() {
    return shardDetails;
  }

  public Completable rxConnect() {
    return this.mysqlVertxClient.rxConnect();
  }

  public Completable rxCloseConnections() {
    return this.mysqlVertxClient.rxClose();
  }
}
