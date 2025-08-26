package com.dream11.shardwizard.example;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.order.CreateOrderResponse;
import com.dream11.shardwizard.example.order.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class OrderAbstractDaoFactoryConfigTest extends BaseShardTest {

  private static final int CONFIGURED_ROUND = 1012;
  private static final int INVALID_ROUND = 1011;
  private static final int USER_ID_1 = 60009005;
  private static final int USER_ID_2 = 60009006;

  @BeforeAll
  public static void setUp() throws Exception {
    setupBase();
  }

  @Test
  void shouldReturnCorrectShardDetailsForValidRound() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            runShardDetailsTest(CONFIGURED_ROUND, latch, error);
          } catch (Exception e) {
            handleTestError("Error in shard details test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  @Test
  void shouldThrowExceptionForInvalidRound() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            runInvalidRoundTest(INVALID_ROUND, latch, error);
          } catch (Exception e) {
            handleTestError("Error in invalid round test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  @Test
  void shouldSaveAndRetrieveOrdersFromCorrectShards() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            runOrderCreationAndVerificationTest(
                CONFIGURED_ROUND, USER_ID_1, USER_ID_2, latch, error);
          } catch (Exception e) {
            handleTestError("Error in order creation test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  private void runShardDetailsTest(
      int roundId, CountDownLatch latch, AtomicReference<Throwable> error) {
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createPostgresShard(1, 5433));
    expectedShardDetails.add(createPostgresShard(2, 5434));

    orderDaoFactory
        .rxGetOrCreateEntityShardDetails(Integer.toString(roundId))
        .subscribe(
            shardDetails -> {
              assertEquals(
                  expectedShardDetails,
                  shardDetails,
                  "Shard details mismatch for round " + roundId);
              latch.countDown();
            },
            e -> handleTestError("Error in shard details test", e, latch, error));
  }

  private void runInvalidRoundTest(
      int invalidRound, CountDownLatch latch, AtomicReference<Throwable> error) {
    orderDaoFactory
        .rxGetEntityShardDetails(Integer.toString(invalidRound))
        .subscribe(
            shardDetails -> {
              error.set(new Exception("Expected exception but got success"));
              latch.countDown();
            },
            e -> {
              log.info(
                  "Expected exception occurred for invalid round {}: {}",
                  invalidRound,
                  e.getMessage());
              latch.countDown();
            });
  }

  private void runOrderCreationAndVerificationTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    log.info("Creating first order for userId: {}, round: {}", userId1, roundId);

    saveOrder(user1Order, roundId, userId1)
        .flatMap(ord1 -> verifyFirstOrder(ord1, roundId, userId1))
        .flatMap(order1DTO -> createAndVerifySecondOrder(roundId, userId2))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in order creation test", e, latch, error));
  }

  private Single<OrderDto> verifyFirstOrder(CreateOrderResponse response, int roundId, int userId) {
    assertNotNull(response, "Order 1 creation response should not be null");
    String orderId = response.getOrderId();
    assertNotNull(orderId, "Order 1 ID should not be null");
    log.info("Created first order with ID: {}", orderId);

    String shardId = extractShardId(orderId);
    log.info("Extracted shard ID: {} from order ID: {}", shardId, orderId);

    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(shardId, userId)
        .doOnSuccess(dao -> log.info("Retrieved DAO instance for shard ID: {}", shardId))
        .doOnError(e -> log.error("Error getting DAO instance for shard ID: {}", shardId, e))
        .flatMap(
            dao -> {
              assertNotNull(dao, "DAO instance for order 1 should not be null");
              log.info("Getting order details for ID: {}", orderId);
              return dao.get(orderId);
            })
        .doOnSuccess(orderDTO -> verifyOrder(orderDTO, orderId, userId));
  }

  private Single<OrderDto> createAndVerifySecondOrder(int roundId, int userId) {
    OrderDto order = createSampleOrder(userId, roundId);
    log.info("Creating second order for userId: {}, round: {}", userId, roundId);

    return saveOrder(order, roundId, userId)
        .flatMap(response -> verifySecondOrder(response, roundId, userId));
  }

  private Single<OrderDto> verifySecondOrder(
      CreateOrderResponse response, int roundId, int userId) {
    assertNotNull(response, "Order 2 creation response should not be null");
    String orderId = response.getOrderId();
    assertNotNull(orderId, "Order 2 ID should not be null");
    log.info("Created second order with ID: {}", orderId);

    String shardId = extractShardId(orderId);
    log.info("Extracted shard ID: {} from order ID: {}", shardId, orderId);

    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(shardId, userId)
        .doOnSuccess(dao -> log.info("Retrieved DAO instance for shard ID: {}", shardId))
        .doOnError(e -> log.error("Error getting DAO instance for shard ID: {}", shardId, e))
        .flatMap(
            dao -> {
              assertNotNull(dao, "DAO instance for order 2 should not be null");
              log.info("Getting order details for ID: {}", orderId);
              return dao.get(orderId);
            })
        .doOnSuccess(orderDTO -> verifyOrder(orderDTO, orderId, userId));
  }

  private String extractShardId(String orderId) {
    String[] parts = orderId.split("-");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid order ID format: " + orderId);
    }
    return parts[2];
  }

  private void verifyOrder(OrderDto order, String expectedOrderId, int expectedUserId) {
    assertNotNull(order, "Retrieved order should not be null");
    assertEquals(expectedOrderId, order.getOrderId(), "Order ID mismatch");
    assertEquals(String.valueOf(expectedUserId), order.getUserId(), "User ID mismatch");
    log.info("Successfully verified order with ID: {}", expectedOrderId);
  }
}
