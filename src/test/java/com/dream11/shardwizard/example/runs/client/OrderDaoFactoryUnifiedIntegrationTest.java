package com.dream11.shardwizard.example.runs.client;

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
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Unified integration test that calls setupBase() once and runs all database-specific tests
 * sequentially for MySQL, PostgreSQL, and DynamoDB. This ensures efficient test execution by
 * avoiding multiple setup/teardown cycles.
 */
@Slf4j
@TestMethodOrder(OrderAnnotation.class)
public class OrderDaoFactoryUnifiedIntegrationTest extends ShardTestSupport { // TODO

  @BeforeAll
  public static void setUpUnified() throws Exception {
    log.info("=== Starting unified test setup ===");
    setupBase();

    // Verify initialization
    if (vertx == null) {
      throw new IllegalStateException("Vertx instance is null after setup");
    }
    if (orderDaoFactory == null) {
      throw new IllegalStateException("OrderDaoFactory is null after setup");
    }

    log.info("=== Unified test setup completed successfully ===");
  }

  // MySQL Test Configuration (CONFIGURED_ROUND = 1023)
  private static final int MYSQL_CONFIGURED_ROUND = 1023;
  private static final int[] MYSQL_TEST_ROUNDS_AND_USER_IDS = {
    MYSQL_CONFIGURED_ROUND, 11125, 60009002
  };

  // PostgreSQL Test Configuration (CONFIGURED_ROUND = 1012)
  private static final int POSTGRES_CONFIGURED_ROUND = 1012;
  private static final int[] POSTGRES_TEST_ROUNDS_AND_USER_IDS = {4446, 11125, 60009002};
  private static final int POSTGRES_SHARD_DETAILS_ROUND = 1009;

  // DynamoDB Test Configuration (CONFIGURED_ROUND = 1013)
  private static final int DYNAMO_CONFIGURED_ROUND = 1013;
  private static final int[] DYNAMO_TEST_ROUNDS_AND_USER_IDS = {
    DYNAMO_CONFIGURED_ROUND, 11125, 60009002
  };

  @Test
  @Order(1)
  void runMySqlTests() throws Exception {
    log.info("=== Running MySQL Integration Tests ===");

    runTestMethod(
        "MySQL - shouldSaveAndRetrieveOrdersAcrossDifferentRounds",
        () -> runMySqlOrderCreationTest());

    runTestMethod(
        "MySQL - shouldReturnCorrectShardDetailsForDifferentRounds",
        () -> runMySqlShardDetailsTest());

    runTestMethod(
        "MySQL - shouldBatchInsertOrdersAndRetrieveThem", () -> runMySqlBatchInsertTest());

    log.info("=== Completed MySQL Integration Tests ===");
  }

  @Test
  @Order(2)
  void runPostgreSqlTests() throws Exception {
    log.info("=== Running PostgreSQL Integration Tests ===");

    runTestMethod(
        "PostgreSQL - shouldSaveAndRetrieveOrdersAcrossDifferentRounds",
        () -> runPostgreSqlOrderCreationTest());

    runTestMethod(
        "PostgreSQL - shouldReturnCorrectShardDetailsForDifferentRounds",
        () -> runPostgreSqlShardDetailsTest());

    log.info("=== Completed PostgreSQL Integration Tests ===");
  }

  @Test
  @Order(3)
  void runDynamoDbTests() throws Exception {
    log.info("=== Running DynamoDB Integration Tests ===");

    runTestMethod(
        "DynamoDB - shouldSaveAndRetrieveOrdersAcrossDifferentRounds",
        () -> runDynamoOrderCreationTest());

    runTestMethod(
        "DynamoDB - shouldReturnCorrectShardDetailsForDifferentRounds",
        () -> runDynamoShardDetailsTest());

    log.info("=== Completed DynamoDB Integration Tests ===");
  }

  @Test
  @Order(4)
  void runAllDatabasesSequentially() throws Exception {
    log.info("=== Running All Database Tests Sequentially ===");

    runMySqlTests();
    runPostgreSqlTests();
    runDynamoDbTests();

    log.info("=== All Database Tests Completed Successfully ===");
  }

  // MySQL-specific test implementations
  private void runMySqlOrderCreationTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = MYSQL_TEST_ROUNDS_AND_USER_IDS[0];
            int userId1 = MYSQL_TEST_ROUNDS_AND_USER_IDS[1];
            int userId2 = MYSQL_TEST_ROUNDS_AND_USER_IDS[2];

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
                      log.info("MySQL order creation test completed successfully");
                      latch.countDown();
                    },
                    e -> handleTestError("Error in MySQL order creation test", e, latch, error));
          } catch (Exception e) {
            handleTestError("Error in MySQL test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "MySQL order creation test failed");
  }

  private void runMySqlShardDetailsTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createMySQLShard(6, 5438));
    expectedShardDetails.add(createMySQLShard(7, 5439));

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDetails(Integer.toString(MYSQL_CONFIGURED_ROUND))
              .subscribe(
                  shardDetails -> {
                    assertEquals(
                        expectedShardDetails,
                        shardDetails,
                        "MySQL shard details mismatch for round " + MYSQL_CONFIGURED_ROUND);
                    log.info("MySQL shard details test completed successfully");
                    latch.countDown();
                  },
                  e -> handleTestError("Error in MySQL shard details test", e, latch, error));
        });

    awaitAndHandleError(latch, error, "MySQL shard details test failed");
  }

  private void runMySqlBatchInsertTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = MYSQL_CONFIGURED_ROUND;
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
                      log.info("MySQL batch insert test completed successfully");
                      latch.countDown();
                    },
                    e -> handleTestError("Error in MySQL batch insert test", e, latch, error));
          } catch (Exception e) {
            handleTestError("Error in MySQL batch insert test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "MySQL batch insert test failed");
  }

  // PostgreSQL-specific test implementations
  private void runPostgreSqlOrderCreationTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = POSTGRES_TEST_ROUNDS_AND_USER_IDS[0];
            int userId1 = POSTGRES_TEST_ROUNDS_AND_USER_IDS[1];
            int userId2 = POSTGRES_TEST_ROUNDS_AND_USER_IDS[2];

            OrderDto user1Order = createSampleOrder(userId1, roundId);
            log.info("Creating first PostgreSQL order for userId: {}, round: {}", userId1, roundId);

            saveOrder(user1Order, roundId, userId1)
                .flatMap(
                    ord1 -> {
                      assertNotNull(ord1, "Order 1 creation response should not be null");
                      String orderId = ord1.getOrderId();
                      assertNotNull(orderId, "Order 1 ID should not be null");

                      String shardId = extractShardId(orderId);
                      return orderDaoFactory
                          .rxGetOrCreateEntityShardDao(shardId, userId1)
                          .flatMap(dao -> dao.get(orderId))
                          .doOnSuccess(orderDTO -> verifyOrder(orderDTO, orderId, userId1));
                    })
                .flatMap(
                    order1DTO -> {
                      OrderDto user2Order = createSampleOrder(userId2, roundId);
                      return saveOrder(user2Order, roundId, userId2)
                          .flatMap(
                              response -> {
                                String orderId = response.getOrderId();
                                String shardId = extractShardId(orderId);
                                return orderDaoFactory
                                    .rxGetOrCreateEntityShardDao(shardId, userId2)
                                    .flatMap(dao -> dao.get(orderId))
                                    .doOnSuccess(
                                        orderDTO -> verifyOrder(orderDTO, orderId, userId2));
                              });
                    })
                .subscribe(
                    order2DTO -> {
                      log.info("PostgreSQL order creation test completed successfully");
                      latch.countDown();
                    },
                    e ->
                        handleTestError(
                            "Error in PostgreSQL order creation test", e, latch, error));
          } catch (Exception e) {
            handleTestError("Error in PostgreSQL test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "PostgreSQL order creation test failed");
  }

  private void runPostgreSqlShardDetailsTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createPostgresShard(3, 5435));
    expectedShardDetails.add(createPostgresShard(4, 5436));

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDetails(Integer.toString(POSTGRES_SHARD_DETAILS_ROUND))
              .subscribe(
                  shardDetails -> {
                    assertEquals(
                        expectedShardDetails,
                        shardDetails,
                        "PostgreSQL shard details mismatch for round "
                            + POSTGRES_SHARD_DETAILS_ROUND);
                    log.info("PostgreSQL shard details test completed successfully");
                    latch.countDown();
                  },
                  e -> handleTestError("Error in PostgreSQL shard details test", e, latch, error));
        });

    awaitAndHandleError(latch, error, "PostgreSQL shard details test failed");
  }

  // DynamoDB-specific test implementations
  private void runDynamoOrderCreationTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = DYNAMO_TEST_ROUNDS_AND_USER_IDS[0];
            int userId1 = DYNAMO_TEST_ROUNDS_AND_USER_IDS[1];
            int userId2 = DYNAMO_TEST_ROUNDS_AND_USER_IDS[2];

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
                      log.info("DynamoDB order creation test completed successfully");
                      latch.countDown();
                    },
                    e -> handleTestError("Error in DynamoDB order creation test", e, latch, error));
          } catch (Exception e) {
            handleTestError("Error in DynamoDB test setup", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "DynamoDB order creation test failed");
  }

  private void runDynamoShardDetailsTest() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createPostgresShard(1, 5433));
    expectedShardDetails.add(createDynamoShard(6, 9111));

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDetails(Integer.toString(DYNAMO_CONFIGURED_ROUND))
              .subscribe(
                  shardDetails -> {
                    assertEquals(
                        expectedShardDetails,
                        shardDetails,
                        "DynamoDB shard details mismatch for round " + DYNAMO_CONFIGURED_ROUND);
                    log.info("DynamoDB shard details test completed successfully");
                    latch.countDown();
                  },
                  e -> handleTestError("Error in DynamoDB shard details test", e, latch, error));
        });

    awaitAndHandleError(latch, error, "DynamoDB shard details test failed");
  }

  // Helper methods
  private void runTestMethod(String testName, TestRunnable testMethod) throws Exception {
    log.info("Starting test: {}", testName);
    try {
      testMethod.run();
      log.info("✅ Test passed: {}", testName);
    } catch (Exception e) {
      log.error("❌ Test failed: {} - Error: {}", testName, e.getMessage(), e);
      throw new Exception("Test failed: " + testName, e);
    }
  }

  private void verifyOrder(OrderDto order, String expectedOrderId, int expectedUserId) {
    assertNotNull(order, "Retrieved order should not be null");
    assertEquals(expectedOrderId, order.getOrderId(), "Order ID mismatch");
    assertEquals(String.valueOf(expectedUserId), order.getUserId(), "User ID mismatch");
  }

  private String extractShardId(String orderId) {
    String[] parts = orderId.split("-");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid order ID format: " + orderId);
    }
    return parts[2];
  }

  private Single<OrderDto> verifyOrders(
      int roundId, int userId1, int userId2, CreateOrderResponseDTO[] responses) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId1)
        .flatMap(dao -> dao.get(responses[0].getOrderId()))
        .doOnSuccess(order1DTO -> verifyOrder(order1DTO, responses[0].getOrderId(), userId1))
        .flatMap(
            order1DTO -> {
              return orderDaoFactory
                  .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId2)
                  .flatMap(dao -> dao.get(responses[1].getOrderId()))
                  .doOnSuccess(
                      order2DTO -> verifyOrder(order2DTO, responses[1].getOrderId(), userId2));
            });
  }

  private Single<List<OrderDto>> verifyBatchOrders(
      int roundId, List<OrderDto> originalOrders, List<CreateOrderResponseDTO> responses) {
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

  @FunctionalInterface
  private interface TestRunnable {
    void run() throws Exception;
  }
}
