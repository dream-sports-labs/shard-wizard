package com.dream11.shardwizard.example.runs.client.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.ShardTestSupport;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Observable;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class OrderDaoFactoryIntegrationTest extends ShardTestSupport {

  private static final int CONFIGURED_ROUND = 1023;
  private static final int INVALID_ROUND = 1011;
  private static final int USER_ID_1 = 60009005;
  private static final int USER_ID_2 = 60009006;

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
  void shouldSaveAndRetrieveOrdersAcrossDifferentRounds() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = 1023;
            int userId1 = 11125;
            int userId2 = 60009002;

            runOrderCreationAndVerificationTest(round1, userId1, userId2, latch, error);
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
  void shouldReturnCorrectShardDetailsForDifferentRounds() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            runShardDetailsTest(1023, latch, error);
          } catch (Exception e) {
            handleTestError("Error in shard details test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  @Test
  /**
   * Test that verifies batch insertion and retrieval of orders for a given round. Should insert
   * multiple orders into the correct shard and confirm retrieval.
   */
  void shouldBatchInsertOrdersAndRetrieveThem() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = 1023;
            List<Integer> userIds = List.of(11125, 60009001, 11124, 60009002, 11123, 60009003);
            List<OrderDto> orders =
                userIds.stream()
                    .map(userId -> createSampleOrder(userId, roundId))
                    .collect(Collectors.toList());

            saveOrderInBatch(orders, roundId)
                .flatMap(
                    responses -> {
                      assertEquals(6, responses.size(), "Expected six orders to be inserted");
                      return verifyBatchOrders(roundId, orders, responses);
                    })
                .subscribe(
                    result -> {
                      log.info("Batch insert and retrieval test passed");
                      latch.countDown();
                    },
                    e -> handleTestError("Error in batch insert test", e, latch, error));
          } catch (Exception e) {
            handleTestError("Unexpected setup error", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Batch insert test failed");
  }

  private Single<List<OrderDto>> verifyBatchOrders(
      int roundId, List<OrderDto> originalOrders, List<CreateOrderResponseDTO> responses) {
    // Map each response back to the userId from original orders
    Map<String, String> orderIdToUserIdMap =
        originalOrders.stream()
            .collect(Collectors.toMap(OrderDto::getOrderId, OrderDto::getUserId));

    return Observable.fromIterable(responses)
        .flatMapSingle(
            response -> {
              String orderId = response.getOrderId();
              String userId = orderIdToUserIdMap.get(orderId);

              return orderDaoFactory
                  .rxGetOrCreateEntityShardDao(String.valueOf(roundId), Integer.parseInt(userId))
                  .flatMap(dao -> dao.get(orderId))
                  .doOnSuccess(
                      fetchedOrder -> verifyOrder(fetchedOrder, orderId, Integer.parseInt(userId)));
            })
        .toList();
  }

  private void runOrderCreationAndVerificationTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    OrderDto user2Order = createSampleOrder(userId2, roundId);

    Single.zip(
            saveOrder(user1Order, roundId, userId1),
            saveOrder(user2Order, roundId, userId2),
            (response1, response2) -> {
              assertNotNull(response1, "Order 1 creation response should not be null");
              assertNotNull(response2, "Order 2 creation response should not be null");
              return new CreateOrderResponseDTO[] {response1, response2};
            })
        .flatMap(responses -> verifyOrders(roundId, userId1, userId2, responses))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in test", e, latch, error));
  }

  private Single<OrderDto> verifyOrders(
      int roundId, int userId1, int userId2, CreateOrderResponseDTO[] responses) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId1)
        .flatMap(dao -> dao.get(responses[0].getOrderId()))
        .doOnSuccess(order1DTO -> verifyOrder(order1DTO, responses[0].getOrderId(), userId1))
        .flatMap(order1DTO -> verifySecondOrder(roundId, userId2, responses[1]));
  }

  private void verifyOrder(OrderDto order, String expectedOrderId, int expectedUserId) {
    assertNotNull(order, "Retrieved order should not be null");
    assertEquals(expectedOrderId, order.getOrderId(), "Order ID mismatch");
    assertEquals(String.valueOf(expectedUserId), order.getUserId(), "User ID mismatch");
  }

  private Single<OrderDto> verifySecondOrder(
      int roundId, int userId, CreateOrderResponseDTO response) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId)
        .flatMap(dao -> dao.get(response.getOrderId()))
        .doOnSuccess(order2DTO -> verifyOrder(order2DTO, response.getOrderId(), userId));
  }

  private void runShardDetailsTest(
      int roundId, CountDownLatch latch, AtomicReference<Throwable> error) {
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createMySQLShard(6, 5438));
    expectedShardDetails.add(createMySQLShard(7, 5439));

    orderDaoFactory
        .rxGetOrCreateEntityShardDetails(Integer.toString(roundId))
        .subscribe(
            shardDetails -> {
              assertEquals(
                  expectedShardDetails,
                  shardDetails,
                  "mysql shard details mismatch for round " + roundId);
              latch.countDown();
            },
            e -> handleTestError("Error in shard details test", e, latch, error));
  }

  @Test
  /**
   * Test that verifies the creation and retrieval of orders across different rounds in a
   * transaction Should save orders in the correct shards and verify they can be retrieved
   */
  void shouldSaveAndRetrieveOrdersAcrossDifferentRoundsInaTransaction() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = 1023;
            int userId1 = 11125;
            int userId2 = 60009002;

            runOrderCreationAndVerificationInTransactionTest(
                round1, userId1, userId2, latch, error);
          } catch (Exception e) {
            handleTestError("Error in test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  private void runOrderCreationAndVerificationInTransactionTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    OrderDto user2Order = createSampleOrder(userId2, roundId);

    Single.zip(
            saveOrderInTransaction(user1Order, roundId, userId1),
            saveOrderInTransaction(user2Order, roundId, userId2),
            (response1, response2) -> {
              assertNotNull(response1, "Order 1 creation response should not be null");
              assertNotNull(response2, "Order 2 creation response should not be null");
              return new CreateOrderResponseDTO[] {response1, response2};
            })
        .flatMap(responses -> verifyOrders(roundId, userId1, userId2, responses))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in test", e, latch, error));
  }

  @Test
  /**
   * Test that verifies the creation and retrieval of orders across different rounds in a
   * transaction Should save orders in the correct shards and verify they can be retrieved
   */
  void shouldSaveOrderInTrx() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = 1023;
            int userId1 = 11125;
            int userId2 = 60009002;

            runOrderCreationAndVerificationTestForTransaction(
                round1, userId1, userId2, latch, error);
          } catch (Exception e) {
            handleTestError("Error in test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  private void runOrderCreationAndVerificationTestForTransaction(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    OrderDto user2Order = createSampleOrder(userId2, roundId);

    Single.zip(
            beginAndCommitOrderInTrx(user1Order, roundId, userId1),
            beginAndCommitOrderInTrx(user2Order, roundId, userId2),
            (response1, response2) -> {
              assertNotNull(response1, "Order 1 creation response should not be null");
              assertNotNull(response2, "Order 2 creation response should not be null");
              return new CreateOrderResponseDTO[] {response1, response2};
            })
        .flatMap(responses -> verifyOrders(roundId, userId1, userId2, responses))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in test", e, latch, error));
  }

  @Test
  /**
   * Test that verifies the creation and retrieval of orders across different rounds with
   * executeQuery Should save orders in the correct shards and verify they can be retrieved
   */
  void shouldSaveAndRetrieveOrdersAcrossDifferentRoundsUsingExecuteQuery() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = 1023;
            int userId1 = 11125;
            int userId2 = 60009002;

            runOrderCreationAndVerificationUsingExecuteQueryTest(
                round1, userId1, userId2, latch, error);
          } catch (Exception e) {
            handleTestError("Error in test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
  }

  private void runOrderCreationAndVerificationUsingExecuteQueryTest(
      int roundId,
      int userId1,
      int userId2,
      CountDownLatch latch,
      AtomicReference<Throwable> error) {
    OrderDto user1Order = createSampleOrder(userId1, roundId);
    OrderDto user2Order = createSampleOrder(userId2, roundId);

    Single.zip(
            saveOrderUsingExecuteQuery(user1Order, roundId, userId1),
            saveOrderUsingExecuteQuery(user2Order, roundId, userId2),
            (response1, response2) -> {
              assertNotNull(response1, "Order 1 creation response should not be null");
              assertNotNull(response2, "Order 2 creation response should not be null");
              return new CreateOrderResponseDTO[] {response1, response2};
            })
        .flatMap(responses -> verifyOrders(roundId, userId1, userId2, responses))
        .subscribe(
            order2DTO -> {
              log.info("Test completed successfully");
              latch.countDown();
            },
            e -> handleTestError("Error in test", e, latch, error));
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

  private Single<OrderDto> verifyFirstOrder(
      CreateOrderResponseDTO response, int roundId, int userId) {
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
      CreateOrderResponseDTO response, int roundId, int userId) {
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
}
