package com.dream11.shardwizard.example.order.impl;

import com.dream11.shardwizard.client.impl.common.RdsCluster;
import com.dream11.shardwizard.dao.impl.postgresvertx.PostgresBaseDao;
import com.dream11.shardwizard.example.order.CreateOrderResponse;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.example.order.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.Tuple;
import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresOrderDaoImpl extends PostgresBaseDao implements OrderDao {

  public PostgresOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
    super(vertx, shardDetails);
  }

  @Override
  public Single<CreateOrderResponse> create(OrderDto orderDto) {
    return
        Single.just(1)
            .flatMap(any -> {
              String orderId = createOrderId(orderDto);
              return postgresVertxClient.rxExecutePreparedQuery(
                      RdsCluster.WRITER,
                      "INSERT INTO orders( order_id, order_name, order_date, order_amount, user_id) VALUES($1, $2, $3, $4, $5)",
                      Tuple.of(
                          orderId,
                          orderDto.getOrderName(),
                          orderDto.getOrderDate(),
                          orderDto.getOrderAmount(),
                          orderDto.getUserId()))
                  .flatMap(
                      result -> {
                        return Single.just(
                            new CreateOrderResponse(orderId));
                      })
                  .doOnSuccess(
                      response -> {
                        log.info("Order Created:" + orderId);
                      })
                  .onErrorResumeNext(
                      throwable -> {
                        log.error(
                            "PostgresOrderDto creation failed:"
                                + throwable);
                        return Single.just(
                            new CreateOrderResponse(orderId));
                      });
            });
  }

  private String createOrderId(OrderDto orderDto) {
    return "ORD" + getShardDetails().getShardId() + "-" + orderDto.getUserId() + "-"
        + orderDto.getRoundId();
  }

  public Single<OrderDto> get(String orderId) {
    return
        Single.just(1)
            .flatMap(any -> {

              return postgresVertxClient.rxExecutePreparedQuery(RdsCluster.WRITER,
                      "select * from orders where order_id = $1",
                      Tuple.of(orderId))
                  .flatMap(
                      rowset -> {
                        List<OrderDto> orderResponseDTOList = new ArrayList<>();
                        for (Row row : rowset) {
                          String order_id = row.getString("order_id");
                          String userId = row.getString("user_id");
                          OrderDto orderResponse = OrderDto.builder()
                              .orderId(order_id)
                              .userId(String.valueOf(userId)).build();
                          orderResponseDTOList.add(orderResponse);
                        }
                        return Single.just(
                            orderResponseDTOList.get(0));
                      });
            });
  }
}
