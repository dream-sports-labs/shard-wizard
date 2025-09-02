package com.dream11.shardwizard.example;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.dto.CreateOrderResponseDTO;
import com.dream11.shardwizard.example.dto.OrderDto;
import com.dream11.shardwizard.example.order.OrderDao;
import com.dream11.shardwizard.example.order.OrderDaoFactory;
import com.dream11.shardwizard.example.utils.AppContext;
import com.dream11.shardwizard.model.CircuitBreakerConfigDTO;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Observable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomUtils;
import software.amazon.awssdk.regions.Region;

@Slf4j
public abstract class BaseShardTest {
  protected static Vertx vertx;
  protected static OrderDaoFactory orderDaoFactory;

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
  protected static final String DEFAULT_ACCESS_KEY = "dummy";
  protected static final String DEFAULT_SECRET_KEY = "dummy";

  // CircuitBreaker Configuration Constants
  protected static final boolean CIRCUIT_BREAKER_ENABLED = true;
  protected static final int CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD = 100;
  protected static final int CIRCUIT_BREAKER_WAIT_DURATION_MS = 10000;
  protected static final int CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE = 10;
  protected static final String CIRCUIT_BREAKER_SLIDING_WINDOW_TYPE = "COUNT_BASED";
  protected static final int CIRCUIT_BREAKER_PERMITTED_CALLS_HALF_OPEN = 10;
  protected static final int CIRCUIT_BREAKER_MIN_CALLS = 100;
  protected static final int CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD = 100;
  protected static final int CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS = 10000;
  protected static final boolean CIRCUIT_BREAKER_DISABLED = false;

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

              log.info("Getting OrderDaoFactory instance from AppContext");
              orderDaoFactory = AppContext.getInstance(OrderDaoFactory.class);

              if (orderDaoFactory == null) {
                String errorMsg =
                    "Failed to get OrderDaoFactory instance from AppContext - getInstance returned null";
                log.error(errorMsg);
                error.set(new IllegalStateException(errorMsg));
                latch.countDown();
                return;
              }

              log.info("OrderDaoFactory instance obtained successfully, starting bootstrap");
              orderDaoFactory
                  .rxBootstrap()
                  .doOnSubscribe(d -> log.info("Bootstrap subscription started"))
                  .doOnComplete(
                      () -> {
                        log.info("OrderDaoFactory bootstrapped successfully");
                        latch.countDown();
                      })
                  .doOnError(
                      e -> {
                        log.error(
                            "Failed to bootstrap OrderDaoFactory due to ==> {} ", e.getMessage());
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
            "OrderDaoFactory is null after setup completion - initialization failed");
      }

      log.info("Test environment setup completed successfully with initialized OrderDaoFactory");
    } catch (Exception e) {
      log.error("Critical error during test setup", e);
      if (orderDaoFactory == null) {
        log.error("OrderDaoFactory is null after error");
      }
      throw e;
    }
  }

  private static CircuitBreakerConfigDTO createCircuitBreakerConfig() {
    return CircuitBreakerConfigDTO.builder()
        .enabled(CIRCUIT_BREAKER_ENABLED)
        .failureRateThreshold(CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD)
        .waitDurationInOpenState(CIRCUIT_BREAKER_WAIT_DURATION_MS)
        .slidingWindowSize(CIRCUIT_BREAKER_SLIDING_WINDOW_SIZE)
        .slidingWindowType(CIRCUIT_BREAKER_SLIDING_WINDOW_TYPE)
        .permittedNumberOfCallsInHalfOpenState(CIRCUIT_BREAKER_PERMITTED_CALLS_HALF_OPEN)
        .minimumNumberOfCalls(CIRCUIT_BREAKER_MIN_CALLS)
        .slowCallRateThreshold(CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD)
        .slowCallDurationThreshold(CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS)
        .build();
  }

  protected static ShardDetails createPostgresShard(int shardId, int port) {
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
            .accessKey(DEFAULT_ACCESS_KEY)
            .secretKey(DEFAULT_SECRET_KEY)
            .region(Region.US_EAST_1.toString())
            .endpoint("http://localhost:" + port)
            .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
            .circuitBreaker(createCircuitBreakerConfig())
            .build();
    ShardConfig shardConfig =
        ShardConfig.builder()
            .shardConnectionParams(shardConnectionParams)
            .databaseType(DatabaseType.POSTGRES)
            .build();
    return ShardDetails.builder().shardId(shardId).shardConfig(shardConfig).build();
  }

  protected static ShardDetails createDynamoShard(int shardId, int port) {

    ShardConnectionParameters shardConnectionParams =
        ShardConnectionParameters.builder()
            .port(port)
            .maxConnections(DEFAULT_MAX_CONNECTIONS)
            .writerHost(DEFAULT_HOST) // or Docker container hostname
            .readerHost(DEFAULT_HOST)
            .database(DEFAULT_POSTGRES_DATABASE) // DynamoDB table name or logical group
            .username(DEFAULT_POSTGRES_USERNAME) // not used but retained for consistency
            .password(DEFAULT_POSTGRES_PASSWORD)
            .maxWaitQueueSize(DEFAULT_MAX_WAIT_QUEUE_SIZE)
            .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
            .accessKey(DEFAULT_ACCESS_KEY)
            .secretKey(DEFAULT_SECRET_KEY)
            .endpoint("http://localhost:" + port)
            .region(Region.US_EAST_1.toString())
            .circuitBreaker(createCircuitBreakerConfig())
            .build();

    ShardConfig shardConfig =
        ShardConfig.builder()
            .shardConnectionParams(shardConnectionParams)
            .databaseType(DatabaseType.DYNAMO)
            .build();

    return ShardDetails.builder().shardId(shardId).shardConfig(shardConfig).build();
  }

  protected static ShardDetails createMySQLShard(int shardId, int port) {
    ShardConnectionParameters shardConnectionParams =
        ShardConnectionParameters.builder()
            .maxConnections(5)
            .port(port)
            .database(DEFAULT_MYSQL_DATABASE)
            .writerHost("localhost")
            .username(DEFAULT_MYSQL_USERNAME)
            .readerHost("localhost")
            .password(DEFAULT_MYSQL_PASSWORD)
            .maxWaitQueueSize(DEFAULT_MAX_WAIT_QUEUE_SIZE)
            .connectionTimeoutMs(DEFAULT_CONNECTION_TIMEOUT_MS)
            .circuitBreaker(createCircuitBreakerConfig())
            .build();
    ShardConfig shardConfig =
        ShardConfig.builder()
            .shardConnectionParams(shardConnectionParams)
            .databaseType(DatabaseType.MYSQL)
            .build();
    return ShardDetails.builder().shardId(shardId).shardConfig(shardConfig).build();
  }

  protected Single<CreateOrderResponseDTO> saveOrder(OrderDto orderDto, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(dao -> dao.create(orderDto));
  }

  protected Single<List<CreateOrderResponseDTO>> saveBulkOrder(
      List<OrderDto> orderDtos, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(dao -> dao.createBulk(orderDtos));
  }

  protected Single<CreateOrderResponseDTO> saveOrderUsingExecuteQuery(
      OrderDto orderDto, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(dao -> dao.rxExecuteQuery(orderDto));
  }

  protected Single<CreateOrderResponseDTO> beginAndCommitOrderInTrx(
      OrderDto orderDto, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(
            dao ->
                dao.rxBeginTxn()
                    .flatMap(
                        trx ->
                            dao.create(orderDto)
                                .flatMap(
                                    res ->
                                        dao.rxCommitTransaction(trx)
                                            .flatMap(response -> Single.just(res)))));
  }

  protected Single<CreateOrderResponseDTO> saveOrderInTransaction(
      OrderDto orderDto, int round, int userId) {
    if (orderDaoFactory == null) {
      return Single.error(
          new IllegalStateException(
              "OrderDaoFactory is not initialized. Make sure setupBase() was called before running tests."));
    }
    return orderDaoFactory
        .rxGetOrCreateEntityShardDao(Integer.toString(round), userId)
        .flatMap(dao -> dao.createTransaction(connection -> dao.create(orderDto)));
  }

  protected Single<List<CreateOrderResponseDTO>> saveOrderInBatch(
      List<OrderDto> orders, int round) {
    if (orderDaoFactory == null) {
      return Single.error(new IllegalStateException("OrderDaoFactory is not initialized."));
    }

    // Step 1: Get shard DAOs per user
    return Observable.fromIterable(orders)
        .flatMapSingle(
            order -> {
              int userId = Integer.parseInt(order.getUserId());
              return orderDaoFactory
                  .rxGetOrCreateEntityShardDao(String.valueOf(round), userId)
                  .map(dao -> Map.entry(dao, order));
            })
        .toList()
        .flatMap(
            daoOrderPairs -> {
              // Step 2: Group orders by DAO instance
              Map<OrderDao, List<OrderDto>> daoToOrdersMap = new HashMap<>();
              for (Map.Entry<OrderDao, OrderDto> entry : daoOrderPairs) {
                daoToOrdersMap
                    .computeIfAbsent(entry.getKey(), k -> new ArrayList<>())
                    .add(entry.getValue());
              }

              // Step 3: Call createBatch on each DAO group
              List<Single<List<CreateOrderResponseDTO>>> batchSaves =
                  daoToOrdersMap.entrySet().stream()
                      .map(entry -> entry.getKey().createBatch(entry.getValue()))
                      .collect(Collectors.toList());

              // Step 4: Zip all batch saves into one response
              return Single.zip(
                  batchSaves,
                  results -> {
                    List<CreateOrderResponseDTO> allResponses = new ArrayList<>();
                    for (Object result : results) {
                      allResponses.addAll((List<CreateOrderResponseDTO>) result);
                    }
                    return allResponses;
                  });
            });
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
