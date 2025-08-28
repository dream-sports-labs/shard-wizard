package com.dream11.shardwizard.circuitbreaker.manager;

import static com.dream11.shardwizard.constant.Constants.SHARD_MANAGER_CONFIG_FOLDER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.dream11.shardwizard.circuitbreaker.exception.CircuitBreakerOpenException;
import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.utils.ConfigUtils;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class CircuitBreakerUtilsTest {

  private static final String SHARD_ID = "test-shard-1";
  private CircuitBreaker circuitBreaker;
  protected ShardManagerConfig shardManagerConfig;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    CircuitBreakerConfig config =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50)
            .waitDurationInOpenState(java.time.Duration.ofMillis(1000))
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .build();
    circuitBreaker = CircuitBreaker.of("test", config);
    this.shardManagerConfig =
        ConfigUtils.fromConfigFile(
            "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/%s.conf", ShardManagerConfig.class);
  }

  @Test
  public void testCircuitBreakerUtilsInstantiation() {
    // Verify that the class can be instantiated
    CircuitBreakerUtils utils = new CircuitBreakerUtils();
    assertNotNull("CircuitBreakerUtils instance should not be null", utils);
  }

  @Test
  public void testWithCircuitBreakerSingle_Success() {

    Single<String> operation = Single.just("success");

    Single<String> result =
        CircuitBreakerUtils.withCircuitBreakerSingle(SHARD_ID, circuitBreaker, () -> operation);

    String value = result.blockingGet();
    assertEquals("success", value);
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
  }

  @Test
  public void testWithCircuitBreakerSingle_Error() {

    RuntimeException error = new RuntimeException("test error");
    Single<String> operation = Single.error(error);

    Single<String> result =
        CircuitBreakerUtils.withCircuitBreakerSingle(SHARD_ID, circuitBreaker, () -> operation);

    try {
      result.blockingGet();
    } catch (RuntimeException e) {
      assertEquals(error, e);
    }
  }

  @Test
  public void testWithCircuitBreakerSingle_CircuitOpen() {

    // Force circuit to open state
    for (int i = 0; i < 10; i++) {
      circuitBreaker.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("test error"));
    }

    Single<String> operation = Single.just("success");

    Single<String> result =
        CircuitBreakerUtils.withCircuitBreakerSingle(SHARD_ID, circuitBreaker, () -> operation);

    try {
      result.blockingGet();
    } catch (CircuitBreakerOpenException e) {
      assertTrue(e.getMessage().contains(SHARD_ID));
    }
  }

  @Test
  public void testWithCircuitBreakerCompletable_Success() {

    Completable operation = Completable.complete();

    Completable result =
        CircuitBreakerUtils.withCircuitBreakerCompletable(
            SHARD_ID, circuitBreaker, () -> operation);

    result.blockingAwait();
    assertEquals(CircuitBreaker.State.CLOSED, circuitBreaker.getState());
  }

  @Test
  public void testWithCircuitBreakerCompletable_Error() {

    RuntimeException error = new RuntimeException("test error");
    Completable operation = Completable.error(error);

    Completable result =
        CircuitBreakerUtils.withCircuitBreakerCompletable(
            SHARD_ID, circuitBreaker, () -> operation);

    try {
      result.blockingAwait();
    } catch (RuntimeException e) {
      assertEquals(error, e);
    }
  }

  @Test
  public void testWithCircuitBreakerCompletable_CircuitOpen() {

    // Force circuit to open state
    for (int i = 0; i < 10; i++) {
      circuitBreaker.onError(0, TimeUnit.MILLISECONDS, new RuntimeException("test error"));
    }

    Completable operation = Completable.complete();

    Completable result =
        CircuitBreakerUtils.withCircuitBreakerCompletable(
            SHARD_ID, circuitBreaker, () -> operation);

    try {
      result.blockingAwait();
    } catch (CircuitBreakerOpenException e) {
      assertTrue(e.getMessage().contains(SHARD_ID));
    }
  }

  @Test
  public void testWithCircuitBreakerCompletable_TimeoutException() {
    TimeoutException timeoutError = new TimeoutException("test timeout");
    Completable operation = Completable.error(timeoutError);

    Completable result =
        CircuitBreakerUtils.withCircuitBreakerCompletable(
            SHARD_ID, circuitBreaker, () -> operation);

    try {
      result.blockingAwait();
    } catch (RuntimeException e) {
      assertEquals(timeoutError, e.getCause());
      assertTrue("Error should be TimeoutException", e.getCause() instanceof TimeoutException);
      // Verify that the error was recorded in the circuit breaker
      assertEquals(1, circuitBreaker.getMetrics().getNumberOfFailedCalls());
    }
  }
}
