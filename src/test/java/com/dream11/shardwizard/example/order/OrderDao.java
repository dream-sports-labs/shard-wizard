package com.dream11.shardwizard.example.order;

import io.reactivex.Single;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import java.util.List;
import java.util.function.Function;

public interface OrderDao {

  Single<CreateOrderResponse> create(OrderDto orderDto);

  Single<List<CreateOrderResponse>> createBulk(List<OrderDto> orderDtos);

  Single<OrderDto> get(String orderId);

  Single<Boolean> update(OrderDto orderDto);

  Single<Boolean> delete(OrderDto orderDto);

  Single<List<CreateOrderResponse>> createBatch(List<OrderDto> orders);

  <T> Single<T> createTransaction(Function<SqlConnection, Single<T>> function);

  Single<Transaction> rxBeginTxn();

  Single<Boolean> rxCommitTransaction(Transaction transaction);

  Single<CreateOrderResponse> rxExecuteQuery(OrderDto orderDto);
}
