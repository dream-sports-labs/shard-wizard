package com.dream11.shardwizard.example.runs.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.ShardTestSupport;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public abstract class OrderDaoFactoryBaseIntegrationTest extends ShardTestSupport {

  // Common constants - to be overridden by concrete classes
  protected static int CONFIGURED_ROUND;
  protected static final int INVALID_ROUND = 1011;
  protected static final int USER_ID_1 = 60009005;
  protected static final int USER_ID_2 = 60009006;

  @BeforeAll
  public static void setUp() throws Exception {
    log.info("Starting test setup");
    setupBase();

    // Verify initialization
    if (vertx == null) {
      throw new IllegalStateException("Vertx instance is null after setup");
    }
    if (orderDaoFactory == null) {
      throw new IllegalStateException("OrderDaoFactory is null after setup");
    }

    log.info("Test setup completed successfully");
  }

  @Test
  /**
   * Test that verifies the creation and retrieval of orders across different rounds Should save
   * orders in the correct shards and verify they can be retrieved
   */
  protected void shouldSaveAndRetrieveOrdersAcrossDifferentRounds() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int[] testParams = getTestRoundsAndUserIds();
            runOrderCreationAndVerificationTest(
                testParams[0], testParams[1], testParams[2], latch, error);
          } catch (Exception e) {
            handleTestError("Error in test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  @Test
  /**
   * Test that verifies the shard configuration for different rounds Should return the correct shard
   * details for each configured round
   */
  protected void shouldReturnCorrectShardDetailsForDifferentRounds() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            runShardDetailsTest(getTestRoundForShardDetails(), latch, error);
          } catch (Exception e) {
            handleTestError("Error in shard details test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  @Test
  protected void shouldReturnCorrectShardDetailsForValidRound() throws Exception {
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
  protected void shouldThrowExceptionForInvalidRound() throws Exception {
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
  protected void shouldSaveAndRetrieveOrdersFromCorrectShards() throws Exception {
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

  // Common verification methods
  protected void verifyOrder(OrderDto order, String expectedOrderId, int expectedUserId) {
    assertNotNull(order, "Retrieved order should not be null");
    assertEquals(expectedOrderId, order.getOrderId(), "Order ID mismatch");
    assertEquals(String.valueOf(expectedUserId), order.getUserId(), "User ID mismatch");
  }

  protected Single<OrderDto> verifySecondOrder(
      int roundId, int userId, CreateOrderResponseDTO response) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId)
        .flatMap(dao -> dao.get(response.getOrderId()))
        .doOnSuccess(order2DTO -> verifyOrder(order2DTO, response.getOrderId(), userId));
  }

  protected void runInvalidRoundTest(
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

  protected String extractShardId(String orderId) {
    String[] parts = orderId.split("-");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid order ID format: " + orderId);
    }
    return parts[2];
  }

  protected Single<OrderDto> verifyFirstOrder(CreateOrderResponseDTO response, int userId) {
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

  protected Single<OrderDto> createAndVerifySecondOrder(int roundId, int userId) {
    OrderDto order = createSampleOrder(userId, roundId);
    log.info("Creating second order for userId: {}, round: {}", userId, roundId);

    return saveOrder(order, roundId, userId)
        .flatMap(response -> verifySecondOrder(response, userId));
  }

  protected Single<OrderDto> verifySecondOrder(CreateOrderResponseDTO response, int userId) {
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

  // Template methods to be implemented by concrete classes
  protected abstract int[] getTestRoundsAndUserIds();

  protected abstract int getTestRoundForShardDetails();

  protected abstract List<ShardDetails> getExpectedShardDetails(int roundId);

  protected abstract void runOrderCreationAndVerificationTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error);

  // Common implementation for runShardDetailsTest with database-specific
  // expectations
  protected void runShardDetailsTest(
      int roundId, CountDownLatch latch, AtomicReference<Throwable> error) {
    List<ShardDetails> expectedShardDetails = getExpectedShardDetails(roundId);

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

  // Common implementation for verifyOrders
  protected Single<OrderDto> verifyOrders(
      int roundId, int userId1, int userId2, CreateOrderResponseDTO[] responses) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId1)
        .flatMap(dao -> dao.get(responses[0].getOrderId()))
        .doOnSuccess(order1DTO -> verifyOrder(order1DTO, responses[0].getOrderId(), userId1))
        .flatMap(order1DTO -> verifySecondOrder(roundId, userId2, responses[1]));
  }
}
