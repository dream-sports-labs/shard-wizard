package com.dream11.shardwizard.circuitbreaker.manager;

import static org.junit.Assert.*;

import com.dream11.shardwizard.dto.CircuitBreakerConfigDTO;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Before;
import org.junit.Test;

public class CircuitBreakerManagerTest {

  private static final String SHARD_ID = "test-shard-1";
  private CircuitBreakerManager circuitBreakerManager;
  private CircuitBreakerConfigDTO config;

  @Before
  public void setup() {
    circuitBreakerManager = CircuitBreakerManager.getInstance();
    config =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(50)
            .waitDurationInOpenState(1000)
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .slidingWindowType("COUNT_BASED")
            .build();
  }

  @Test
  public void testGetCircuitBreaker_CreatesNewInstance() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    assertNotNull(breaker);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testGetCircuitBreaker_ReturnsExistingInstance() {
    CircuitBreaker firstInstance = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    CircuitBreaker secondInstance = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    assertSame(firstInstance, secondInstance);
  }

  @Test
  public void testGetCircuitBreaker_NullConfig() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, null);
    assertNull("Circuit breaker should be null when config is null", breaker);
  }

  @Test
  public void testGetCircuitBreaker_WhenDisabled() {
    CircuitBreakerConfigDTO disabledConfig =
        CircuitBreakerConfigDTO.builder().enabled(false).build();

    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, disabledConfig);
    assertNull("Circuit breaker should be null when disabled", breaker);
  }

  @Test
  public void testGetCircuitBreaker_WhenDisabledThenEnabled() {
    // First get with disabled config
    CircuitBreakerConfigDTO disabledConfig =
        CircuitBreakerConfigDTO.builder().enabled(false).build();

    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, disabledConfig);
    assertNull("Circuit breaker should be null when disabled", breaker);

    // Then get with enabled config
    CircuitBreakerConfigDTO enabledConfig = CircuitBreakerConfigDTO.builder().enabled(true).build();

    breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, enabledConfig);
    assertNotNull("Circuit breaker should be created when enabled", breaker);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testDisableCircuitBreaker() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    circuitBreakerManager.disableCircuitBreaker(SHARD_ID);
    assertEquals(CircuitBreaker.State.DISABLED, breaker.getState());
  }

  @Test
  public void testEnableCircuitBreaker() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    circuitBreakerManager.disableCircuitBreaker(SHARD_ID);
    circuitBreakerManager.enableCircuitBreaker(SHARD_ID);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testResetCircuitBreaker() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);
    for (int i = 0; i < 5; i++) {
      breaker.onSuccess(0, java.util.concurrent.TimeUnit.MILLISECONDS);
    }
    for (int i = 0; i < 6; i++) {
      breaker.onError(
          0, java.util.concurrent.TimeUnit.MILLISECONDS, new ConnectException("test error"));
    }

    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
    circuitBreakerManager.resetCircuitBreaker(SHARD_ID);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testShouldRecordException() throws Exception {
    // Get the private method using reflection
    java.lang.reflect.Method method =
        CircuitBreakerManager.class.getDeclaredMethod("shouldRecordException", Throwable.class);
    method.setAccessible(true);

    // Create an instance of CircuitBreakerManager
    CircuitBreakerManager manager = CircuitBreakerManager.getInstance();

    // Test exceptions that should be recorded
    assertTrue((Boolean) method.invoke(manager, new ConnectException()));
    assertTrue((Boolean) method.invoke(manager, new SocketTimeoutException()));
    assertTrue((Boolean) method.invoke(manager, new TimeoutException()));

    // Test exceptions that should not be recorded
    assertFalse((Boolean) method.invoke(manager, new RuntimeException()));
    assertFalse((Boolean) method.invoke(manager, new IllegalArgumentException()));
  }

  @Test
  public void testSlowCallCircuitBreaker() {
    CircuitBreakerConfigDTO slowCallConfig =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(50)
            .waitDurationInOpenState(1000)
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .slidingWindowType("COUNT_BASED")
            .slowCallRateThreshold(50)
            .slowCallDurationThreshold(50)
            .build();

    CircuitBreaker breaker =
        circuitBreakerManager.getCircuitBreaker("slow-call-test", slowCallConfig);
    assertNotNull(breaker);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Simulate slow calls
    for (int i = 0; i < 10; i++) {
      breaker.onSuccess(100, TimeUnit.MILLISECONDS); // Calls taking 100ms (above threshold)
    }

    // Wait for a short time to ensure state transition
    try {
      Thread.sleep(100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    // Circuit should be open due to slow calls
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  public void testCircuitBreakerHalfOpenToOpen() throws InterruptedException {
    CircuitBreakerConfigDTO transitionConfig =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(50)
            .waitDurationInOpenState(100)
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .slidingWindowType("COUNT_BASED")
            .build();

    CircuitBreaker breaker =
        circuitBreakerManager.getCircuitBreaker("half-open-test", transitionConfig);

    // Force OPEN state
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);

    for (int i = 0; i < 6; i++) {
      breaker.onError(0, TimeUnit.MILLISECONDS, new ConnectException("test error"));
    }

    // Wait for OPEN state to be established
    Thread.sleep(50);
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    Thread.sleep(150);

    breaker.tryAcquirePermission();

    // Verify HALF-OPEN state
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    // Test HALF-OPEN -> OPEN transition (failed test calls)
    breaker.onError(0, TimeUnit.MILLISECONDS, new ConnectException("test error"));
    breaker.onError(0, TimeUnit.MILLISECONDS, new ConnectException("test error"));

    // Verify OPEN state
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());
  }

  @Test
  public void testCircuitBreakerOpenToCloseTransition() throws InterruptedException {
    CircuitBreakerConfigDTO transitionConfig =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(50)
            .waitDurationInOpenState(50) // Short wait duration for testing
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .slidingWindowType("COUNT_BASED")
            .build();

    CircuitBreaker breaker =
        circuitBreakerManager.getCircuitBreaker("transition-test", transitionConfig);

    for (int i = 0; i < 5; i++) {
      breaker.onError(0, TimeUnit.MILLISECONDS, new ConnectException("test error"));
    }

    // Wait for state transition
    Thread.sleep(50);
    assertEquals(CircuitBreaker.State.OPEN, breaker.getState());

    // Wait for OPEN -> HALF-OPEN transition
    Thread.sleep(200);
    breaker.tryAcquirePermission();
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);
    assertEquals(CircuitBreaker.State.HALF_OPEN, breaker.getState());

    // Test HALF-OPEN -> CLOSED transition (successful test calls)
    breaker.tryAcquirePermission();
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testCircuitBreakerDisabledState() {
    CircuitBreaker breaker = circuitBreakerManager.getCircuitBreaker(SHARD_ID, config);

    // Disable the circuit breaker
    circuitBreakerManager.disableCircuitBreaker(SHARD_ID);
    assertEquals(CircuitBreaker.State.DISABLED, breaker.getState());

    // Verify it stays in DISABLED state even after successful calls
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);
    assertEquals(CircuitBreaker.State.DISABLED, breaker.getState());

    // Enable the circuit breaker
    circuitBreakerManager.enableCircuitBreaker(SHARD_ID);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testCircuitBreakerForcedOpen() {
    CircuitBreakerConfigDTO transitionConfig =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(50)
            .waitDurationInOpenState(100)
            .permittedNumberOfCallsInHalfOpenState(2)
            .slidingWindowSize(10)
            .minimumNumberOfCalls(5)
            .slidingWindowType("COUNT_BASED")
            .build();

    CircuitBreaker breaker =
        circuitBreakerManager.getCircuitBreaker("forced-open-test", transitionConfig);

    // Verify initial CLOSED state
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());

    // Force open the circuit breaker
    breaker.transitionToForcedOpenState();

    // Verify FORCED_OPEN state
    assertEquals(CircuitBreaker.State.FORCED_OPEN, breaker.getState());

    // Verify it stays in FORCED_OPEN state even after successful calls
    breaker.onSuccess(0, TimeUnit.MILLISECONDS);
    assertEquals(CircuitBreaker.State.FORCED_OPEN, breaker.getState());

    // Reset the circuit breaker
    breaker.reset();

    // Verify it goes back to CLOSED state
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }

  @Test
  public void testGetCircuitBreaker_WithNullValues() {
    CircuitBreakerConfigDTO configWithNullSlidingWindow =
        CircuitBreakerConfigDTO.builder()
            .enabled(true)
            .failureRateThreshold(null)
            .waitDurationInOpenState(null)
            .permittedNumberOfCallsInHalfOpenState(null)
            .slidingWindowSize(null)
            .minimumNumberOfCalls(null)
            .slidingWindowType(null)
            .build();

    CircuitBreaker breaker =
        circuitBreakerManager.getCircuitBreaker(
            "null-sliding-window-test", configWithNullSlidingWindow);
    assertNotNull(breaker);
    assertEquals(CircuitBreaker.State.CLOSED, breaker.getState());
  }
}
