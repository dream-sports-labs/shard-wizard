package com.dream11.shardwizard.dao.impl;

import com.dream11.shardwizard.client.DynamoVertxClient;
import com.dream11.shardwizard.client.impl.DynamoVertxClientImpl;
import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class DynamoBaseDao extends BaseDaoAbstract {

  private final ShardDetails shardDetails;
  protected DynamoVertxClient dynamoVertxClient;

  protected final Vertx vertx;

  protected DynamoBaseDao(Vertx vertx, ShardDetails shardDetails) {
    this.vertx = vertx;
    this.shardDetails = shardDetails;

    ShardConnectionParameters connectionParams =
        shardDetails.getShardConfig().getShardConnectionParams();

    String endpoint = connectionParams.getEndpoint();
    String region = connectionParams.getRegion();

    log.info("Initializing DynamoDB client for region: {} and endpoint {} ", region, endpoint);
    this.dynamoVertxClient =
        new DynamoVertxClientImpl(
            vertx,
            endpoint,
            region,
            connectionParams.getAccessKey(),
            connectionParams.getSecretKey(),
            shardDetails);
  }

  @Override
  public ShardDetails getShardDetails() {
    return shardDetails;
  }

  public Completable rxConnect() {
    return dynamoVertxClient.rxConnect();
  }

  public Completable rxCloseConnections() {
    return dynamoVertxClient.rxClose();
  }
}
