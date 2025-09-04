package com.dream11.shardwizard.example.order.impl;

import com.dream11.shardwizard.dao.BaseDaoAbstract;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import java.util.List;
import java.util.function.Function;

public class CustomDatabaseOrderDaoImpl extends BaseDaoAbstract
    implements OrderDao { // todo - not used anywhere

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
  public Single<CreateOrderResponseDTO> create(OrderDto orderDto) {
    System.out.println("MysqlOrderDto created");
    return Single.just(new CreateOrderResponseDTO());
  }

  @Override
  public Single<List<CreateOrderResponseDTO>> createBulk(List<OrderDto> orderDtos) {
    return null;
  }

  @Override
  public Single<OrderDto> get(String orderId) {
    System.out.println("Fetching Mysql orderDto");
    return Single.just(new OrderDto());
  }

  @Override
  public Single<List<CreateOrderResponseDTO>> createBatch(List<OrderDto> orders) {
    System.out.println("MysqlOrderDto created in batch");
    return Single.just(List.of(new CreateOrderResponseDTO()));
  }

  @Override
  public <T> Single<T> createTransaction(Function<SqlConnection, Single<T>> function) {
    // Todo: need to implement this in postgres
    return null;
  }

  @Override
  public Single<Transaction> rxBeginTxn() {
    // Todo: need to implement this in postgres
    return null;
  }

  @Override
  public Single<Boolean> rxCommitTransaction(Transaction transaction) {
    // Todo: need to implement this in postgres
    return null;
  }

  @Override
  public Single<CreateOrderResponseDTO> rxExecuteQuery(OrderDto orderDto) {
    return null;
  }

  @Override
  public Single<Boolean> update(OrderDto orderDto) {
    return null;
  }

  @Override
  public Single<Boolean> delete(OrderDto orderDto) {
    return null;
  }
}
