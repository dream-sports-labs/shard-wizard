package com.dream11.shardwizard.example.order;

import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import io.reactivex.Single;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import java.util.List;
import java.util.function.Function;

public interface OrderDao {

  Single<CreateOrderResponseDTO> create(OrderDto orderDto);

  Single<List<CreateOrderResponseDTO>> createBulk(List<OrderDto> orderDtos);

  Single<OrderDto> get(String orderId);

  Single<Boolean> update(OrderDto orderDto);

  Single<Boolean> delete(OrderDto orderDto);

  Single<List<CreateOrderResponseDTO>> createBatch(List<OrderDto> orders);

  <T> Single<T> createTransaction(Function<SqlConnection, Single<T>> function);

  Single<Transaction> rxBeginTxn();

  Single<Boolean> rxCommitTransaction(Transaction transaction);

  Single<CreateOrderResponseDTO> rxExecuteQuery(OrderDto orderDto);
}
