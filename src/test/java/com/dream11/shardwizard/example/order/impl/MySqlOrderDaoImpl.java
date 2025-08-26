package com.dream11.shardwizard.example.order.impl;

import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.dao.impl.MySqlVertxBaseDao;
import com.dream11.shardwizard.example.order.CreateOrderResponse;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.example.order.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.sqlclient.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MySqlOrderDaoImpl extends MySqlVertxBaseDao implements OrderDao {

  public MySqlOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
    super(vertx, shardDetails);
  }

  @Override
  public Single<CreateOrderResponse> create(OrderDto orderDto) {
    return Single.just(1)
        .flatMap(
            any -> {
              String orderId = createOrderId(orderDto);
              log.info("orderId: {}", orderId);
              return mysqlVertxClient
                  .rxExecutePreparedQuery(
                      RdsCluster.WRITER,
                      "INSERT INTO orders(order_id, order_name, order_date, order_amount, user_id) VALUES(?, ?, ?, ?, ?)",
                      Tuple.of(
                          orderId,
                          orderDto.getOrderName(),
                          orderDto.getOrderDate(),
                          orderDto.getOrderAmount(),
                          orderDto.getUserId()))
                  .flatMap(
                      result -> {
                        log.info("Order Created before: {}", orderId);
                        return Single.just(new CreateOrderResponse(orderId));
                      })
                  .doOnSuccess(
                      response -> {
                        log.info("Order Created: {}", orderId);
                      })
                  .onErrorResumeNext(
                      throwable -> {
                        log.error("MySqlOrderDto creation failed: {}", throwable.getMessage());
                        return Single.just(new CreateOrderResponse(orderId));
                      });
            });
  }

  @Override
  public Single<List<CreateOrderResponse>> createBulk(List<OrderDto> orderDtos) {
    return null;
  }

  private String createOrderId(OrderDto orderDto) {
    return "ORD"
        + getShardDetails().getShardId()
        + "-"
        + orderDto.getUserId()
        + "-"
        + orderDto.getRoundId();
  }

  @Override
  public Single<OrderDto> get(String orderId) {
    return Single.just(1)
        .flatMap(
            any ->
                mysqlVertxClient
                    .rxExecutePreparedQuery(
                        RdsCluster.READER,
                        "SELECT * FROM orders WHERE order_id = ?",
                        Tuple.of(orderId))
                    .flatMap(
                        rowset -> {
                          log.info("Order fetched before: {}", orderId);
                          List<OrderDto> orderResponseDTOList = new ArrayList<>();
                          for (Row row : rowset) {
                            String order_id = row.getString("order_id");
                            String userId = row.getString("user_id");
                            OrderDto orderResponse =
                                OrderDto.builder()
                                    .orderId(order_id)
                                    .userId(String.valueOf(userId))
                                    .build();
                            orderResponseDTOList.add(orderResponse);
                          }
                          return Single.just(orderResponseDTOList.get(0));
                        }))
        .doOnSuccess(
            response -> {
              log.info("Order verified: {}", orderId);
            })
        .onErrorResumeNext(
            throwable -> {
              log.error("MySqlOrderDto verification failed: {}", throwable.getMessage());
              return Single.just(OrderDto.builder().orderId(orderId).build());
            });
  }

  @Override
  public Single<Boolean> update(OrderDto orderDto) {
    return null;
  }

  @Override
  public Single<Boolean> delete(OrderDto orderDto) {
    return null;
  }

  @Override
  public Single<List<CreateOrderResponse>> createBatch(List<OrderDto> orders) {
    String query =
        "INSERT INTO orders(order_id, order_name, order_date, order_amount, user_id) VALUES (?, ?, ?, ?, ?)";
    List<Tuple> tuples = new ArrayList<>();

    for (OrderDto order : orders) {
      String orderId = createOrderId(order);
      order.setOrderId(orderId);
      tuples.add(
          Tuple.of(
              orderId,
              order.getOrderName(),
              order.getOrderDate(),
              order.getOrderAmount(),
              order.getUserId()));
    }

    return mysqlVertxClient
        .rxExecuteBatchPreparedQuery(RdsCluster.WRITER, query, tuples)
        .map(
            rowSet -> {
              List<CreateOrderResponse> responses = new ArrayList<>();
              for (OrderDto order : orders) {
                responses.add(new CreateOrderResponse(order.getOrderId()));
              }

              return responses;
            })
        .doOnSuccess(responses -> log.info("Successfully inserted {} orders", responses.size()))
        .doOnError(e -> log.error("Batch insert failed", e));
  }

  @Override
  public <T> Single<T> createTransaction(Function<SqlConnection, Single<T>> function) {
    return mysqlVertxClient.rxGetConnectionForTxn(function);
  }

  @Override
  public Single<Transaction> rxBeginTxn() {
    return mysqlVertxClient.rxBeginTxn();
  }

  @Override
  public Single<Boolean> rxCommitTransaction(Transaction transaction) {
    return mysqlVertxClient
        .rxCommitTransaction(transaction)
        .doOnSuccess(success -> log.info("Transaction committed successfully"))
        .doOnError(error -> log.error("Error committing transaction: {}", error.getMessage()));
  }

  @Override
  public Single<CreateOrderResponse> rxExecuteQuery(OrderDto orderDto) {
    return Single.just(1)
        .flatMap(
            any -> {
              String orderId = createOrderId(orderDto);
              log.info("orderId: {}", orderId);
              String query =
                  String.format(
                      "INSERT INTO orders(order_id, order_name, order_date, order_amount, user_id) "
                          + "VALUES('%s', '%s', '%s', %.2f, '%s')",
                      orderId,
                      orderDto.getOrderName(),
                      orderDto.getOrderDate().toString(), // format if needed
                      orderDto.getOrderAmount(),
                      orderDto.getUserId());
              return mysqlVertxClient
                  .rxExecuteQuery(RdsCluster.WRITER, query)
                  .flatMap(
                      result -> {
                        log.info("Order Created before: {}", orderId);
                        return Single.just(new CreateOrderResponse(orderId));
                      })
                  .doOnSuccess(
                      response -> {
                        log.info("Order Created: {}", orderId);
                      })
                  .onErrorResumeNext(
                      throwable -> {
                        log.error("MySqlOrderDto creation failed: {}", throwable);
                        return Single.just(new CreateOrderResponse(orderId));
                      });
            });
  }
  ;
}
