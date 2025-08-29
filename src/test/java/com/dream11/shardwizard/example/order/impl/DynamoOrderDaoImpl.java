package com.dream11.shardwizard.example.order.impl;

import static com.dream11.shardwizard.example.utils.Constants.TABLE_NAME;

import com.dream11.shardwizard.dao.impl.dynamo.DynamoBaseDao;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;

@Slf4j
public class DynamoOrderDaoImpl extends DynamoBaseDao implements OrderDao {

  public DynamoOrderDaoImpl(Vertx vertx, ShardDetails shardDetails) {
    super(vertx, shardDetails);
  }

  @Override
  public Single<CreateOrderResponseDTO> create(OrderDto orderDto) {
    return Single.defer(
        () -> {
          String orderId = createOrderId(orderDto);

          Map<String, AttributeValue> item = new HashMap<>();
          item.put(
              "order_id", AttributeValue.builder().s(orderId).build()); // ✅ matches partition key
          item.put("order_name", AttributeValue.builder().s(orderDto.getOrderName()).build());
          item.put("order_date", AttributeValue.builder().s(orderDto.getOrderDate()).build());
          item.put(
              "order_amount",
              AttributeValue.builder().n(String.valueOf(orderDto.getOrderAmount())).build());
          item.put("user_id", AttributeValue.builder().s(orderDto.getUserId()).build());

          return dynamoVertxClient
              .rxWriteItem(TABLE_NAME, item)
              .map(ignored -> new CreateOrderResponseDTO(orderId))
              .doOnSuccess(resp -> log.info("Order Created: {}", orderId))
              .onErrorReturn(
                  throwable -> {
                    log.error("Failed to create order in DynamoDB {}", throwable.getMessage());
                    return new CreateOrderResponseDTO(orderId);
                  });
        });
  }

  @Override
  public Single<Boolean> update(OrderDto orderDto) {
    return Single.defer(
        () -> {
          String orderId = orderDto.getOrderId(); // must match partition key

          // Partition key (must match table definition)
          Map<String, AttributeValue> key =
              Map.of("order_id", AttributeValue.builder().s(orderId).build());

          // Generate random 2–3 character alphanumeric prefix
          String randomPrefix = getRandomSuffix();

          // Attributes to update (modified for test)
          Map<String, AttributeValueUpdate> updates =
              Map.of(
                  "order_name",
                      AttributeValueUpdate.builder()
                          .value(
                              AttributeValue.builder()
                                  .s(randomPrefix + orderDto.getOrderName())
                                  .build())
                          .action(AttributeAction.PUT)
                          .build(),
                  "order_date",
                      AttributeValueUpdate.builder()
                          .value(
                              AttributeValue.builder()
                                  .s(randomPrefix + orderDto.getOrderDate())
                                  .build())
                          .action(AttributeAction.PUT)
                          .build(),
                  "order_amount",
                      AttributeValueUpdate.builder()
                          .value(
                              AttributeValue.builder()
                                  .n(String.valueOf(orderDto.getOrderAmount()))
                                  .build())
                          .action(AttributeAction.PUT)
                          .build(),
                  "user_id",
                      AttributeValueUpdate.builder()
                          .value(
                              AttributeValue.builder()
                                  .s(randomPrefix + orderDto.getUserId())
                                  .build())
                          .action(AttributeAction.PUT)
                          .build());

          return dynamoVertxClient
              .rxUpdateItem(TABLE_NAME, key, updates)
              .doOnSuccess(success -> log.info("✅ Order updated with test data: {}", orderId))
              .onErrorReturn(
                  error -> {
                    log.error("❌ Failed to update order in DynamoDB {}", error.getMessage());
                    return false;
                  });
        });
  }

  @Override
  public Single<Boolean> delete(OrderDto orderDto) {
    return Single.defer(
        () -> {
          String orderId = orderDto.getOrderId(); // must match partition key

          Map<String, AttributeValue> key =
              Map.of("order_id", AttributeValue.builder().s(orderId).build());

          return dynamoVertxClient
              .rxDeleteItem(TABLE_NAME, key)
              .doOnSuccess(success -> log.info("✅ Order deleted: {}", orderId))
              .onErrorReturn(
                  error -> {
                    log.error("❌ Failed to delete order from DynamoDB {}", error.getMessage());
                    return false;
                  });
        });
  }

  @Override
  public Single<List<CreateOrderResponseDTO>> createBatch(List<OrderDto> orders) {
    return null;
  }

  @Override
  public <T> Single<T> createTransaction(Function<SqlConnection, Single<T>> function) {
    return null;
  }

  @Override
  public Single<Transaction> rxBeginTxn() {
    return null;
  }

  @Override
  public Single<Boolean> rxCommitTransaction(Transaction transaction) {
    return null;
  }

  @Override
  public Single<CreateOrderResponseDTO> rxExecuteQuery(OrderDto orderDto) {
    return null;
  }

  @Override
  public Single<OrderDto> get(String orderId) {
    Map<String, AttributeValue> key =
        Map.of(
            "order_id", AttributeValue.builder().s(orderId).build() // ✅ Use correct partition key
            );

    return dynamoVertxClient
        .rxGetItem(TABLE_NAME, key)
        .map(
            item -> {
              if (item == null || item.isEmpty()) {
                throw new RuntimeException("Order not found: " + orderId);
              }

              OrderDto orderDto =
                  OrderDto.builder()
                      .orderId(
                          item.containsKey("order_id")
                              ? item.get("order_id").s()
                              : orderId) // fallback
                      .orderName(item.get("order_name").s())
                      .orderDate(item.get("order_date").s())
                      .orderAmount(Double.parseDouble(item.get("order_amount").n()))
                      .userId(item.get("user_id").s())
                      .build();

              log.info("✅ Order retrieved: {}", orderDto.getOrderId());
              return orderDto;
            });
  }

  @Override
  public Single<List<CreateOrderResponseDTO>> createBulk(List<OrderDto> orderDtos) {
    return Single.defer(
        () -> {
          List<Map<String, AttributeValue>> itemsBatch = new ArrayList<>();
          List<CreateOrderResponseDTO> responses = new ArrayList<>();
          for (OrderDto dto : orderDtos) {
            String orderId = createOrderId(dto);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("order_id", AttributeValue.builder().s(orderId).build()); // partition key
            item.put("order_name", AttributeValue.builder().s(dto.getOrderName()).build());
            item.put("order_date", AttributeValue.builder().s(dto.getOrderDate()).build());
            item.put(
                "order_amount",
                AttributeValue.builder().n(String.valueOf(dto.getOrderAmount())).build());
            item.put("user_id", AttributeValue.builder().s(dto.getUserId()).build());

            itemsBatch.add(item);
            responses.add(new CreateOrderResponseDTO(orderId));
          }

          return dynamoVertxClient
              .rxBatchWriteItem(TABLE_NAME, itemsBatch)
              .map(
                  success -> {
                    if (!success) {
                      throw new RuntimeException("Batch write failed");
                    }
                    return responses;
                  })
              .doOnSuccess(
                  result ->
                      log.info("✅ Batch order creation succeeded with {} items", result.size()))
              .onErrorReturn(
                  throwable -> {
                    log.error("❌ Batch order creation failed", throwable);
                    return responses; // optionally return partial IDs
                  });
        });
  }

  private String createOrderId(OrderDto orderDto) {
    return "ORD"
        + getRandomSuffix()
        + getShardDetails().getShardId()
        + "-"
        + orderDto.getUserId()
        + "-"
        + orderDto.getRoundId();
  }

  private static String getRandomSuffix() {
    String randomSuffix = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 4);
    return randomSuffix;
  }
}
