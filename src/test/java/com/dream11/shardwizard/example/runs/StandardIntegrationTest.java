package com.dream11.shardwizard.example.runs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dream11.shardwizard.example.ShardTestSupport;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.exception.EntityNotMappedToShardException;
import com.dream11.shardwizard.exception.ShardNotPresentException;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class StandardIntegrationTest
    extends ShardTestSupport { // TODO - TO CHECK - THIS IS WRITTEN ACCORDING TO POSTGRES
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
  void shouldSaveOrdersAndReturnOrdersAndShardDetailsInDefaultShards() throws Exception {
    int round1 = 1111; /* Is Not configured on any shard, shard 3 and 4 are default */
    List<ShardDetails> shardDetailsList = new ArrayList<>();
    shardDetailsList.add(createPostgresShard(3, 5435));
    shardDetailsList.add(createPostgresShard(4, 5436));

    int userId1 = 60009001;
    int userId2 = 60009002;

    runTestWithOrders(round1, shardDetailsList, userId1, userId2);
  }

  @Test
  void shouldSaveOrdersAndReturnOrdersAndShardDetailsInTwoDifferentShards() throws Exception {
    int round1 = 1024; /* Configured on shard 1(Postgres), 6(Mysql) that are active */
    List<ShardDetails> shardDetailsList = new ArrayList<>();
    shardDetailsList.add(createPostgresShard(1, 5433));
    shardDetailsList.add(createMySQLShard(6, 5438));
    int userId1 = 60009011;
    int userId2 = 60009022;
    runTestWithOrders(round1, shardDetailsList, userId1, userId2);
  }

  @Test
  void shouldSaveOrdersAndReturnOrdersAndShardDetailsInSameShards() throws Exception {
    int round1 = 2222; /* Configured on shard 1, 2 that are active */
    List<ShardDetails> shardDetailsList = new ArrayList<>();
    shardDetailsList.add(createPostgresShard(1, 5433));
    shardDetailsList.add(createPostgresShard(2, 5434));

    int userId1 = 60009011;
    int userId2 = 60009022;

    runTestWithOrders(round1, shardDetailsList, userId1, userId2);
  }

  @Test
  void shouldThrowExceptionWhenShardIsInactiveAndGetShardDetailsCalled() throws Throwable {
    int round1 = 3333; /* Configured on shard 5 that is inActive */
    runExceptionTest(
        round1,
        ShardNotPresentException.class,
        "Expected ShardNotPresentException but got success");
  }

  @Test
  void shouldThrowExceptionWhenShardIsInactiveAndGetOrCreateDaoIsCalled() throws Throwable {
    int round1 = 3333; /* Configured on shard 5 that is inActive */
    runExceptionTest(
        round1,
        ShardNotPresentException.class,
        "Expected ShardNotPresentException but got success");
  }

  @Test
  void shouldThrowExceptionWhenShardIsInactiveAndGetOrCreateShardDetailsIsCalled()
      throws Throwable {
    int round1 = 3333; /* Configured on shard 5 that is inActive */
    runExceptionTest(
        round1,
        ShardNotPresentException.class,
        "Expected ShardNotPresentException but got success");
  }

  @Test
  void shouldThrowExceptionNoMappingIsPresentAndGetShardDetailsIsCalled() throws Throwable {
    int round1 = 4444; /* Is Not configured */
    runExceptionTest(
        round1,
        EntityNotMappedToShardException.class,
        "Expected EntityNotMappedToShardException but got success");
  }

  @Test
  void shouldCreateEntityMappingWhenGetOrCreateShardDetailsIsCalled() throws Throwable {
    int round1 = 5555; /* Is Not configured and will be mapped to default shard 3 and 4*/
    List<ShardDetails> expectedShardDetails = new ArrayList<>();
    expectedShardDetails.add(createPostgresShard(3, 5435));
    expectedShardDetails.add(createPostgresShard(4, 5436));

    runShardDetailsTest(round1, expectedShardDetails);
  }

  private void runTestWithOrders(
      int roundId, List<ShardDetails> expectedShardDetails, int userId1, int userId2)
      throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    log.info("Starting test with orders for round: {}, users: {}, {}", roundId, userId1, userId2);

    vertx.runOnContext(
        v -> {
          try {
            OrderDto user1ShardOrder = createSampleOrder(userId1, roundId);
            Single<CreateOrderResponseDTO> ord1 = saveOrder(user1ShardOrder, roundId, userId1);

            OrderDto user2ShardOrder = createSampleOrder(userId2, roundId);
            Single<CreateOrderResponseDTO> ord2 = saveOrder(user2ShardOrder, roundId, userId2);

            // Chain all operations together
            Single.zip(
                    ord1,
                    ord2,
                    orderDaoFactory.rxGetOrCreateEntityShardDetails(Integer.toString(roundId)),
                    (response1, response2, shardDetails) -> {
                      assertEquals(shardDetails, expectedShardDetails);
                      return response1;
                    })
                .flatMap(response1 -> verifyOrder(roundId, userId1, response1.getOrderId()))
                .flatMap(
                    order1DTO -> {
                      assertEquals(order1DTO.getUserId(), String.valueOf(userId1));
                      return ord2;
                    })
                .flatMap(response2 -> verifyOrder(roundId, userId2, response2.getOrderId()))
                .subscribe(
                    order2DTO -> {
                      assertEquals(order2DTO.getUserId(), String.valueOf(userId2));
                      latch.countDown();
                    },
                    e -> {
                      error.set(e);
                      latch.countDown();
                    });
          } catch (Exception e) {
            error.set(e);
            latch.countDown();
          }
        });

    awaitAndHandleError(latch, error);
  }

  private Single<OrderDto> verifyOrder(int roundId, int userId, String orderId) {
    return orderDaoFactory
        .rxGetOrCreateEntityShardDetails(Integer.toString(roundId))
        .flatMap(
            shardDetails ->
                orderDaoFactory
                    .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId)
                    .flatMap(dao -> dao.get(orderId)));
  }

  private void runExceptionTest(
      int roundId, Class<? extends Throwable> expectedException, String errorMessage)
      throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            log.info("Attempting to get shard details for round ID: {}", roundId);
            orderDaoFactory
                .rxGetEntityShardDetails(String.valueOf(roundId))
                .subscribe(
                    shardDetails -> {
                      log.error("Unexpected success - shard details found: {}", shardDetails);
                      error.set(new RuntimeException(errorMessage));
                      latch.countDown();
                    },
                    e -> {
                      Throwable cause = e;
                      if (e instanceof CompletionException) {
                        cause = e.getCause();
                      }

                      if (expectedException.isInstance(cause)) {
                        log.info(
                            "Expected {} occurred: {}",
                            expectedException.getSimpleName(),
                            cause.getMessage());
                        latch.countDown();
                      } else {
                        log.error("Unexpected exception occurred", cause);
                        error.set(cause);
                        latch.countDown();
                      }
                    });
          } catch (Exception e) {
            log.error("Exception occurred while getting shard details", e);
            error.set(e);
            latch.countDown();
          }
        });

    awaitAndHandleError(latch, error);
  }

  private void runShardDetailsTest(int roundId, List<ShardDetails> expectedShardDetails)
      throws Throwable {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            log.info("Attempting to get or create shard details for round ID: {}", roundId);
            orderDaoFactory
                .rxGetOrCreateEntityShardDetails(String.valueOf(roundId))
                .subscribe(
                    shardDetails -> {
                      try {
                        log.info("Received shard details: {}", shardDetails);
                        assertEquals(expectedShardDetails, shardDetails);
                        latch.countDown();
                      } catch (AssertionError e) {
                        error.set(e);
                        latch.countDown();
                      }
                    },
                    e -> {
                      log.error("Error occurred while getting shard details", e);
                      error.set(e);
                      latch.countDown();
                    });
          } catch (Exception e) {
            log.error("Exception occurred while getting shard details", e);
            error.set(e);
            latch.countDown();
          }
        });

    awaitAndHandleError(latch, error);
  }
}
