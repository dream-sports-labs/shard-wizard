package com.dream11.shardwizard.example.runs.dynamo;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.BaseShardTest;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
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
public class OrderDaoFactoryIntegrationTest extends BaseShardTest {

  private static final int CONFIGURED_ROUND = 1013;

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
            int round1 = CONFIGURED_ROUND;
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
            runShardDetailsTest(CONFIGURED_ROUND, latch, error);
          } catch (Exception e) {
            handleTestError("Error in shard details test", e, latch, error);
          }
        });

    awaitAndHandleError(latch, error, "Test failed");
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
    expectedShardDetails.add(createPostgresShard(1, 5433));
    expectedShardDetails.add(createDynamoShard(6, 9111));

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
}
