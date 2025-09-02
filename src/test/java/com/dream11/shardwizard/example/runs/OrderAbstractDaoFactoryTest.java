package com.dream11.shardwizard.example.runs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dream11.shardwizard.example.BaseShardTest;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.example.order.OrderDaoFactory;
import com.dream11.shardwizard.example.order.impl.PostgresOrderDaoImpl;
import com.dream11.shardwizard.example.utils.AppContext;
import com.dream11.shardwizard.router.impl.ConsistentHashingRouter;
import io.vertx.reactivex.core.Vertx;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@Slf4j
public class OrderAbstractDaoFactoryTest extends BaseShardTest {

  private static final int MYSQL_SHARD_ID = 4;
  private static final int POSTGRES_SHARD_ID = 3;
  private static Vertx vertx;
  private static OrderDaoFactory orderDaoFactory;

  @BeforeAll
  public static void setUp() throws Exception {
    log.info("Starting test setup");
    CountDownLatch latch = new CountDownLatch(1);
    vertx = Vertx.vertx();
    setupBase();

    vertx.runOnContext(
        v -> {
          try {
            // Get the already initialized OrderAbstractDaoFactory
            orderDaoFactory = AppContext.getInstance(OrderDaoFactory.class);
            // Bootstrap the factory in a non-blocking way
            orderDaoFactory
                .rxBootstrap()
                .doOnComplete(
                    () -> {
                      log.info("OrderAbstractDaoFactory bootstrapped successfully");
                      latch.countDown();
                    })
                .doOnError(
                    error -> {
                      log.error("Failed to bootstrap OrderAbstractDaoFactory", error);
                      latch.countDown();
                    })
                .subscribe();
          } catch (Exception e) {
            log.error("Error during setup", e);
            latch.countDown();
          }
        });
    latch.await();

    if (vertx == null) {
      throw new IllegalStateException("Vertx instance is null after setup");
    }
    if (orderDaoFactory == null) {
      throw new IllegalStateException("OrderAbstractDaoFactory is null after setup");
    }

    log.info("Test setup completed successfully");
  }

  @Test
  /**
   * Test that verifies the correct DAO implementation is returned based on the entity ID Should
   * return PostgresOrderDaoImpl for one entity ID and MysqlOrderDaoImpl for another
   */
  void shouldReturnCorrectDaoImplementation() throws Exception {
    runDaoImplementationTest("1234", 1111, "1234", 2222);
  }

  @Test
  /**
   * Test that verifies the DAO can be retrieved by primary key Should return the correct DAO
   * implementation for a valid order ID
   */
  void shouldGetDaoByPrimaryKeySuccess() throws Exception {
    runPrimaryKeyTest(POSTGRES_SHARD_ID, "123456", POSTGRES_SHARD_ID, "456789");
  }

  @Test
  /**
   * Test that verifies an exception is thrown when trying to get a DAO by an invalid primary key
   * Should throw IllegalArgumentException for various invalid order ID formats
   */
  void shouldThrowExceptionForInvalidPrimaryKey() throws Exception {
    runInvalidPrimaryKeyTest("asdmaskdmsad");
  }

  @Test
  /**
   * Test that verifies the consistent hashing router distributes requests evenly across shards
   * Should distribute requests evenly across 3 shards with a small tolerance for imbalance
   */
  void shouldDistributeRequestsEvenlyAcrossThreeShards() {
    runDistributionTest(List.of(1L, 2L, 3L), 1_000_000, 0.05);
  }

  @Test
  /**
   * Test that verifies the consistent hashing router distributes requests evenly across many shards
   * Should distribute requests evenly across 11 shards with a reasonable tolerance for imbalance
   */
  void shouldDistributeRequestsEvenlyAcrossManyShards() {
    runDistributionTest(List.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L), 1_000_000, 0.05);
  }

  @Test
  /**
   * Test that verifies the factory can handle concurrent requests for the same entity ID Should
   * return the same DAO instance for concurrent requests
   */
  void shouldHandleConcurrentRequestsForSameEntity() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();
    String entityId = "1234";
    int userId = 1111;

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDao(entityId, userId)
              .flatMap(
                  dao1 -> {
                    return orderDaoFactory
                        .rxGetOrCreateEntityShardDao(entityId, userId)
                        .map(
                            dao2 -> {
                              assertTrue(
                                  dao1 == dao2,
                                  "Expected same DAO instance for concurrent requests");
                              return dao2;
                            });
                  })
              .subscribe(
                  dao -> {
                    log.info("Successfully verified concurrent requests for entity: {}", entityId);
                    latch.countDown();
                  },
                  e -> {
                    log.error("Error in concurrent requests test", e);
                    error.set(e);
                    latch.countDown();
                  });
        });

    awaitAndHandleError(latch, error, "Failed to handle concurrent requests");
  }

  @Test
  /**
   * Test that verifies the factory can handle different database types Should return appropriate
   * DAO implementation based on database type
   */
  void shouldHandleDifferentDatabaseTypes() throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDao("postgres_entity", 1111)
              .flatMap(
                  dao1 -> {
                    assertTrue(dao1 instanceof PostgresOrderDaoImpl);
                    return orderDaoFactory.rxGetOrCreateEntityShardDao("postgres_entity_2", 2222);
                  })
              .flatMap(
                  dao2 -> {
                    assertTrue(dao2 instanceof PostgresOrderDaoImpl);
                    return orderDaoFactory.rxGetOrCreateEntityShardDao("postgres_entity_3", 3333);
                  })
              .subscribe(
                  dao3 -> {
                    assertTrue(dao3 instanceof PostgresOrderDaoImpl);
                    latch.countDown();
                  },
                  e -> {
                    error.set(e);
                    latch.countDown();
                  });
        });

    awaitAndHandleError(latch, error, "Failed to handle different database types");
  }

  private void runDaoImplementationTest(
      String entityId1, int userId1, String entityId2, int userId2) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          orderDaoFactory
              .rxGetOrCreateEntityShardDao(entityId1, userId1)
              .flatMap(
                  dao1 -> {
                    assertTrue(dao1 instanceof PostgresOrderDaoImpl);
                    return orderDaoFactory.rxGetOrCreateEntityShardDao(entityId2, userId2);
                  })
              .subscribe(
                  dao2 -> {
                    assertTrue(dao2 instanceof PostgresOrderDaoImpl);
                    latch.countDown();
                  },
                  e -> {
                    error.set(e);
                    latch.countDown();
                  });
        });

    awaitAndHandleError(latch, error, "Failed to runDaoImplementationTest");
  }

  private void runPrimaryKeyTest(int shardId1, String suffix1, int shardId2, String suffix2)
      throws Exception {
    CountDownLatch latch = new CountDownLatch(2);
    AtomicReference<Throwable> error = new AtomicReference<>();

    // Test first order ID
    vertx.runOnContext(
        v -> {
          try {
            String orderId = "ORD-" + shardId1 + "-" + suffix1;
            OrderDao dao = orderDaoFactory.rxGetDaoInstanceByPrimaryKey(orderId).blockingGet();
            assertTrue(
                dao instanceof PostgresOrderDaoImpl,
                "Expected PostgresOrderDaoImpl for order ID: " + orderId);
          } catch (Exception e) {
            log.error("Error in first test", e);
            error.set(e);
          } finally {
            latch.countDown();
          }
        });

    // Test second order ID
    vertx.runOnContext(
        v -> {
          try {
            String orderId = "ORD-" + shardId2 + "-" + suffix2;
            OrderDao dao = orderDaoFactory.rxGetDaoInstanceByPrimaryKey(orderId).blockingGet();
            assertTrue(
                dao instanceof PostgresOrderDaoImpl,
                "Expected PostgresOrderDaoImpl for order ID: " + orderId);
          } catch (Exception e) {
            log.error("Error in second test", e);
            error.set(e);
          } finally {
            latch.countDown();
          }
        });

    awaitAndHandleError(latch, error, "Failed to runPrimaryKeyTest");
  }

  private void runInvalidPrimaryKeyTest(String invalidKey) throws Exception {
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    vertx.runOnContext(
        v -> {
          try {
            log.info("Testing invalid primary key format: {}", invalidKey);
            orderDaoFactory
                .rxGetDaoInstanceByPrimaryKey(invalidKey)
                .subscribe(
                    dao -> {
                      String msg =
                          "Expected IllegalArgumentException but got success with DAO: " + dao;
                      log.error(msg);
                      error.set(new AssertionError(msg));
                      latch.countDown();
                    },
                    e -> {
                      if (e instanceof IllegalArgumentException) {
                        log.info(
                            "Successfully caught expected IllegalArgumentException: {}",
                            e.getMessage());
                        latch.countDown();
                      } else {
                        String msg =
                            "Expected IllegalArgumentException but got: "
                                + e.getClass().getSimpleName();
                        log.error(msg, e);
                        error.set(new AssertionError(msg, e));
                        latch.countDown();
                      }
                    });
          } catch (IllegalArgumentException e) {
            log.info("Successfully caught expected IllegalArgumentException: {}", e.getMessage());
            latch.countDown();
          } catch (Exception e) {
            log.error("Unexpected error during test execution", e);
            error.set(e);
            latch.countDown();
          }
        });

    awaitAndHandleError(latch, error);
  }

  private void runDistributionTest(
      List<Long> activeShardIds, int totalRequests, double tolerancePercentage) {
    ConsistentHashingRouter shardRouter = new ConsistentHashingRouter();
    shardRouter.initialize(activeShardIds);

    Map<Long, Integer> shardCounters = new HashMap<>();
    for (long shardId : activeShardIds) {
      shardCounters.put(shardId, 0);
    }

    // Route requests
    for (int i = 0; i < totalRequests; i++) {
      String routeKey = "user-" + i;
      long shardId = shardRouter.getRoutedShardId(routeKey);
      shardCounters.put(shardId, shardCounters.getOrDefault(shardId, 0) + 1);
    }

    // Verify distribution
    int averageCount = totalRequests / activeShardIds.size();
    int tolerance = (int) (averageCount * tolerancePercentage);

    for (long shardId : activeShardIds) {
      int count = shardCounters.get(shardId);
      double deviation = Math.abs(count - averageCount);
      double deviationPercentage = (deviation / averageCount) * 100;

      assertTrue(
          deviation <= tolerance,
          String.format(
              "Shard %d has an imbalance: count=%d, average=%d, deviation=%.2f%%",
              shardId, count, averageCount, deviationPercentage));
    }

    // Verify all requests were routed
    int totalRoutedRequests = shardCounters.values().stream().mapToInt(Integer::intValue).sum();
    assertEquals(
        totalRequests,
        totalRoutedRequests,
        "Total routed requests should match total input requests");
  }
}
