package com.dream11.shardwizard.example;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.order.CreateOrderResponse;
import com.dream11.shardwizard.example.order.OrderAbstractDaoFactory;
import com.dream11.shardwizard.example.order.OrderDto;
import com.dream11.shardwizard.example.utils.AppContext;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;

@Slf4j
public abstract class BaseShardTest {

  protected static final int DEFAULT_MAX_CONNECTIONS = 5;
  protected static final int DEFAULT_CONNECTION_TIMEOUT_MS = 500;
  protected static final int DEFAULT_MAX_WAIT_QUEUE_SIZE = 50;
  protected static final String DEFAULT_POSTGRES_DATABASE = "postgres";
  protected static final String DEFAULT_POSTGRES_USERNAME = "postgres";
  protected static final String DEFAULT_POSTGRES_PASSWORD = "postgres";
  protected static final String DEFAULT_MYSQL_DATABASE = "mysql";
  protected static final String DEFAULT_MYSQL_USERNAME = "mysql";
  protected static final String DEFAULT_MYSQL_PASSWORD = "mysql";
  protected static final String DEFAULT_HOST = "localhost";
  protected static Vertx vertx;
  protected static OrderAbstractDaoFactory orderDaoFactory;

  protected static void setupBase() throws Exception {
    // Set test environment
    System.setProperty("app.environment", "test");
    CountDownLatch latch = new CountDownLatch(1);
    AtomicReference<Throwable> error = new AtomicReference<>();

    try {
      log.info("Starting base test setup");

      vertx = Vertx.vertx();
      log.info("Vertx instance initialized successfully");

      StandardIntegrationTestSetup.setup();
      log.info("Standard integration test setup completed");

      vertx.runOnContext(
          v -> {
            try {
              log.info("Initializing AppContext");
              AppContext.initialize(new MainModule());
              log.info("AppContext initialized successfully");

              log.info("Getting OrderAbstractDaoFactory instance from AppContext");
              orderDaoFactory = AppContext.getInstance(OrderAbstractDaoFactory.class);

              if (orderDaoFactory == null) {
                String errorMsg =
                    "Failed to get OrderAbstractDaoFactory instance from AppContext - getInstance returned null";
                log.error(errorMsg);
                error.set(new IllegalStateException(errorMsg));
                latch.countDown();
                return;
              }

              log.info(
                  "OrderAbstractDaoFactory instance obtained successfully, starting bootstrap");
              orderDaoFactory
                  .rxBootstrap()
                  .doOnSubscribe(d -> log.info("Bootstrap subscription started"))
                  .doOnComplete(
                      () -> {
                        log.info("OrderAbstractDaoFactory bootstrapped successfully");
                        latch.countDown();
                      })
                  .doOnError(
                      e -> {
                        log.error("Failed to bootstrap OrderAbstractDaoFactory", e);
                        error.set(e);
                        latch.countDown();
                      })
                  .subscribe();
            } catch (Exception e) {
              log.error("Error during setup within Vertx context", e);
              error.set(e);
              latch.countDown();
            }
          });

      log.info("Waiting for setup completion");
      awaitAndHandleError(latch, error, "Failed to setup test environment");

      if (orderDaoFactory == null) {
        throw new IllegalStateException(
            "OrderAbstractDaoFactory is null after setup completion - initialization failed");
      }

      log.info(
          "Test environment setup completed successfully with initialized OrderAbstractDaoFactory");
    } catch (Exception e) {
      log.error("Critical error during test setup", e);
      if (orderDaoFactory == null) {
        log.error("OrderAbstractDaoFactory is null after error");
      }
      throw e;
    }
  }

  protected static ShardDetails createPOSTGresShard(int shardId, int port) {
    ShardConnectionParameters shardConnectionParams =
        ShardConnectionParameters.builder()
            .maxConnections(DEFAULT_MAX_CONNECTIONS)
            .port(port)
            .database(DEFAULT_POSTGRES_DATABASE)
            .writerHost(DEFAULT_HOST)
            .username(DEFAULT_POSTGRES_USERNAME)
            .readerHost(DEFAULT_HOST)
            .password(DEFAULT_POSTGRES_PASSWORD)
            .maxWaitQueueSize(DEFAULT_MAX_WAIT_QUEUE_SIZE)
            .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
            .build();
    ShardConfig shardConfig =
        ShardConfig.builder()
            .shardConnectionParams(shardConnectionParams)
            .databaseType(DatabaseType.POSTGRES)
            .build();
    return ShardDetails.builder().shardId(shardId).shardConfig(shardConfig).build();
  }

  private static ShardDetails createMySQLShard(int shardId, int port) {
    ShardConnectionParameters shardConnectionParams =
        ShardConnectionParameters.builder()
            .maxConnections(5)
            .port(port)
            .database(DEFAULT_MYSQL_DATABASE)
            .writerHost("localhost")
            .username(DEFAULT_MYSQL_USERNAME)
            .readerHost("localhost")
            .password(DEFAULT_MYSQL_PASSWORD)
            .build();
    ShardConfig shardConfig =
        ShardConfig.builder()
            .shardConnectionParams(shardConnectionParams)
            .databaseType(DatabaseType.MYSQL)
            .build();
    return ShardDetails.builder().shardId(shardId).shardConfig(shardConfig).build();
  }

  protected static void awaitAndHandleError(
      CountDownLatch latch, AtomicReference<Throwable> error, String errorMessage)
      throws Exception {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Exception("Test interrupted", e);
    }

    if (error.get() != null) {
      throw new Exception(errorMessage, error.get());
    }
  }

  protected Single<CreateOrderResponse> saveOrder(OrderDto orderDto, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderAbstractDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(dao -> dao.create(orderDto));
  }

  protected OrderDto createSampleOrder(int userId, int roundId) {
    return OrderDto.builder()
        .orderAmount(RandomUtils.nextDouble(100, 1000))
        .orderName("MyOrder For user " + userId + " round " + roundId)
        .orderDate(Instant.now().toString())
        .orderStatus("PENDING")
        .orderTime(Instant.now().toString())
        .orderType("ONLINE")
        .userId(String.valueOf(userId))
        .roundId(String.valueOf(roundId))
        .build();
  }

  protected void awaitAndHandleError(CountDownLatch latch, AtomicReference<Throwable> error)
      throws Exception {
    try {
      latch.await();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new Exception("Test interrupted", e);
    }

    if (error.get() != null) {
      if (error.get() instanceof Exception) {
        throw (Exception) error.get();
      } else {
        throw new Exception(error.get());
      }
    }
  }

  protected void handleTestError(
      String message, Throwable e, CountDownLatch latch, AtomicReference<Throwable> error) {
    log.error(message, e);
    error.set(e);
    latch.countDown();
  }
}
