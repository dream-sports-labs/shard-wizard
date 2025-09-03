package com.dream11.shardwizard.example.runs.client.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.runs.client.OrderDaoFactoryBaseIntegrationTest;
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
import org.junit.jupiter.api.Test;

@Slf4j
public class OrderDaoFactoryIntegrationTest extends OrderDaoFactoryBaseIntegrationTest {

  static {
    CONFIGURED_ROUND = 1023;
  }

  // MySQL-specific implementation methods
  @Override
  protected int[] getTestRoundsAndUserIds() {
    return new int[] {CONFIGURED_ROUND, 11125, 60009002};
  }

  @Override
  protected int getTestRoundForShardDetails() {
    return CONFIGURED_ROUND;
  }

  @Override
  protected List<ShardDetails> getExpectedShardDetails(int roundId) {
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createMySQLShard(6, 5438));
    expectedShardDetails.add(createMySQLShard(7, 5439));
    return expectedShardDetails;
  }

  @Override
  protected void runOrderCreationAndVerificationTest(
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

  @Test
  /**
   * Test that verifies batch insertion and retrieval of orders for a given round. Should insert
   * multiple orders into the correct shard and confirm retrieval.
   */
  protected void shouldBatchInsertOrdersAndRetrieveThem() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int roundId = CONFIGURED_ROUND;
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

  @Test
  /**
   * Test that verifies the creation and retrieval of orders across different rounds in a
   * transaction Should save orders in the correct shards and verify they can be retrieved
   */
  protected void shouldSaveAndRetrieveOrdersAcrossDifferentRoundsInaTransaction() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = CONFIGURED_ROUND;
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
  protected void shouldSaveOrderInTrx() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = CONFIGURED_ROUND;
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
  protected void shouldSaveAndRetrieveOrdersAcrossDifferentRoundsUsingExecuteQuery()
      throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            int round1 = CONFIGURED_ROUND;
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
}
