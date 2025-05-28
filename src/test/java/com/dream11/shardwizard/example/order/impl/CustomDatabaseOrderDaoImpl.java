package com.dream11.shardwizard.example.order.impl;

import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.example.order.CreateOrderResponse;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.example.order.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;

public class CustomDatabaseOrderDaoImpl extends BaseDaoAbstract implements OrderDao {

  private String customDbClient;

  public CustomDatabaseOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
    int port = shardDetails.getShardConfig().getShardConnectionParams().getPort();
    int maxConnections =
        shardDetails.getShardConfig().getShardConnectionParams().getMaxConnections();

    // You can use data stored in additional params to create connection to your custom DB client
  }

  @Override
  public Completable rxConnect() {
    // you should implement this to create connection to your custom DB client
    return null;
  }

  @Override
  public Completable rxCloseConnections() {
    // you should implement this to close connections of your custom DB client
    return null;
  }

  @Override
  public ShardDetails getShardDetails() {
    return null;
  }

  @Override
  public Single<CreateOrderResponse> create(OrderDto orderDto) {
    System.out.println("MysqlOrderDto created");
    return Single.just(new CreateOrderResponse());
  }

  @Override
  public Single<OrderDto> get(String orderId) {
    System.out.println("Fetching Mysql orderDto");
    return Single.just(new OrderDto());
  }
}
