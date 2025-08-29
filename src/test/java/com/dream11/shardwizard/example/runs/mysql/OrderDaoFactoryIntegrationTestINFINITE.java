package com.dream11.shardwizard.example.runs.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dream11.shardwizard.example.BaseShardTest;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import io.reactivex.Single;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class OrderDaoFactoryIntegrationTestINFINITE extends BaseShardTest {
  private final AtomicLong totalOrdersCreated = new AtomicLong(0);
  private final AtomicLong totalErrors = new AtomicLong(0);
  private static final int BATCH_DELAY_MS = 1000;
  private static final int PROGRESS_LOG_INTERVAL = 100;
  private static final int INITIAL_ROUND_ID = 1023;
  private static final int INITIAL_USER_ID = 1;

  @BeforeAll
  public static void setUp() throws Exception {
    setupBase();
  }

  @Test
  /**
   * Test that continuously creates orders across multiple rounds This is an infinite test that will
   * keep running until manually stopped
   */
  void shouldCreateOrdersContinuously() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicInteger roundId = new AtomicInteger(INITIAL_ROUND_ID);
    AtomicInteger userId = new AtomicInteger(INITIAL_USER_ID);

    log.info("Starting infinite order creation test");
    vertx.runOnContext(v -> createOrdersBatch(roundId, userId, latch));

    latch.await();
  }

  private void createOrdersBatch(
      AtomicInteger roundId, AtomicInteger userId, CountDownLatch latch) {
    try {
      createAndVerifyOrder(roundId.get(), userId.get())
          .doOnError(e -> handleOrderCreationError(roundId.get(), userId.get(), e))
          .doFinally(() -> scheduleNextBatch(roundId, userId, latch))
          .subscribe();
    } catch (Exception e) {
      handleBatchError(roundId, userId, latch, e);
    }
  }

  private Single<OrderDto> createAndVerifyOrder(int roundId, int userId) {
    return Single.fromCallable(() -> createSampleOrder(userId, roundId))
        .flatMap(orderDto -> saveOrder(orderDto, roundId, userId))
        .flatMap(response -> verifyOrder(response, roundId, userId));
  }

  private Single<OrderDto> verifyOrder(CreateOrderResponseDTO response, int roundId, int userId) {
    log.info("Starting verify order creation test");
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(roundId), userId)
        .flatMap(dao -> dao.get(response.getOrderId()))
        .doOnSuccess(
            retrievedOrder -> {
              assertEquals(retrievedOrder.getOrderId(), response.getOrderId());
              assertEquals(retrievedOrder.getUserId(), String.valueOf(userId));
              totalOrdersCreated.incrementAndGet();

              logOrderCreationSuccess(response.getOrderId(), userId, roundId);
            });
  }

  private void logOrderCreationSuccess(String orderId, int userId, int roundId) {
    log.info(
        "Created order {} for user {} in round {} (Total: {}, Errors: {})",
        orderId,
        userId,
        roundId,
        totalOrdersCreated.get(),
        totalErrors.get());
  }

  private void handleOrderCreationError(int roundId, int userId, Throwable e) {
    log.error("Error creating order for user {} in round {}", userId, roundId, e);
    totalErrors.incrementAndGet();
  }

  private void scheduleNextBatch(
      AtomicInteger roundId, AtomicInteger userId, CountDownLatch latch) {
    vertx.setTimer(
        BATCH_DELAY_MS,
        timerId -> {
          userId.incrementAndGet();
          roundId.incrementAndGet();

          logProgressIfNeeded(userId.get());
          createOrdersBatch(roundId, userId, latch);
        });
  }

  private void logProgressIfNeeded(int currentUserId) {
    if (currentUserId % PROGRESS_LOG_INTERVAL == 0) {
      log.info(
          "Processed {} users so far (Total Orders: {}, Errors: {})",
          currentUserId,
          totalOrdersCreated.get(),
          totalErrors.get());
    }
  }

  private void handleBatchError(
      AtomicInteger roundId, AtomicInteger userId, CountDownLatch latch, Exception e) {
    log.error("Error in order creation batch", e);
    totalErrors.incrementAndGet();
    vertx.setTimer(BATCH_DELAY_MS, timerId -> createOrdersBatch(roundId, userId, latch));
  }
}
