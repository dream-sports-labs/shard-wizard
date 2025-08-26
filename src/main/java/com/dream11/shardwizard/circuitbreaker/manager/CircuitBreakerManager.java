package com.dream11.shardwizard.circuitbreaker.manager;

import com.dream11.shardwizard.dto.CircuitBreakerConfigDTO;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType;
import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages circuit breaker instances and provides utility methods for circuit breaker operations.
 * This class is responsible for: 1. Creating and managing circuit breaker instances for different
 * shards 2. Configuring circuit breaker behavior 3. Providing utility methods to wrap operations
 * with circuit breaker functionality 4. Handling circuit breaker state transitions and events
 */
@Slf4j
public class CircuitBreakerManager {
  private final Map<String, CircuitBreaker> shardCircuitBreakers;

  private CircuitBreakerManager() {
    this.shardCircuitBreakers = new ConcurrentHashMap<>();
  }

  private static class SingletonHolder {
    private static final CircuitBreakerManager INSTANCE = new CircuitBreakerManager();
  }

  public static CircuitBreakerManager getInstance() {
    return SingletonHolder.INSTANCE;
  }

  private CircuitBreakerConfig createCustomConfig(CircuitBreakerConfigDTO config) {
    CircuitBreakerConfig.Builder builder =
        CircuitBreakerConfig.custom()
            .recordException(this::shouldRecordException)
            .ignoreExceptions(IllegalArgumentException.class);

    if (config.getFailureRateThreshold() != null) {
      builder.failureRateThreshold(config.getFailureRateThreshold().floatValue());
    }

    if (config.getWaitDurationInOpenState() != null) {
      builder.waitDurationInOpenState(Duration.ofMillis(config.getWaitDurationInOpenState()));
    }

    if (config.getPermittedNumberOfCallsInHalfOpenState() != null) {
      builder.permittedNumberOfCallsInHalfOpenState(
          config.getPermittedNumberOfCallsInHalfOpenState());
    }

    if (config.getSlidingWindowSize() != null) {
      builder.slidingWindowSize(config.getSlidingWindowSize());
    }

    if (config.getMinimumNumberOfCalls() != null) {
      builder.minimumNumberOfCalls(config.getMinimumNumberOfCalls());
    }

    if (config.getSlidingWindowType() != null) {
      builder.slidingWindowType(SlidingWindowType.valueOf(config.getSlidingWindowType()));
    }

    if (config.getSlowCallRateThreshold() != null) {
      builder.slowCallRateThreshold(config.getSlowCallRateThreshold().floatValue());
    }

    if (config.getSlowCallDurationThreshold() != null) {
      builder.slowCallDurationThreshold(Duration.ofMillis(config.getSlowCallDurationThreshold()));
    }

    return builder.build();
  }

  /**
   * Determines if an exception should be recorded for circuit breaker metrics. Records exceptions
   * that indicate connection or timeout issues.
   *
   * @param throwable The exception to check
   * @return true if the exception should be recorded, false otherwise
   */
  private boolean shouldRecordException(Throwable throwable) {
    return throwable instanceof ConnectException
        || throwable instanceof SocketTimeoutException
        || throwable instanceof TimeoutException;
  }

  /**
   * Gets or creates a circuit breaker instance for the specified shard. If disableCircuitBreaker is
   * true in the config, no circuit breaker will be created.
   *
   * @param shardId The shard ID
   * @param config Configuration for the circuit breaker
   * @return CircuitBreaker instance for the shard, or null if circuit breaker is disabled
   */
  public CircuitBreaker getCircuitBreaker(String shardId, CircuitBreakerConfigDTO config) {

    // If circuit breaker is disabled in config or config itself is null, return null
    if (config == null || Boolean.FALSE.equals(config.getEnabled())) {
      log.info("Circuit breaker is disabled for shard {}", shardId);
      return null;
    }
    return shardCircuitBreakers.computeIfAbsent(
        shardId,
        id -> {
          CircuitBreaker breaker = CircuitBreaker.of(id, createCustomConfig(config));
          setupCircuitBreakerListeners(breaker, id);
          return breaker;
        });
  }

  private void setupCircuitBreakerListeners(CircuitBreaker breaker, String shardId) {
    breaker
        .getEventPublisher()
        .onStateTransition(
            event -> {
              switch (event.getStateTransition()) {
                case CLOSED_TO_OPEN:
                  log.warn(
                      "Circuit breaker for shard {} opened due to high failure rate, failureRate: {}, slowCallRate: {}",
                      shardId,
                      breaker.getMetrics().getFailureRate(),
                      breaker.getMetrics().getSlowCallRate());
                  break;
                case OPEN_TO_HALF_OPEN:
                  log.info(
                      "Circuit breaker for shard {} is now half-open, testing connection", shardId);
                  break;
                case HALF_OPEN_TO_CLOSED:
                  log.info(
                      "Circuit breaker for shard {} closed after successful test calls", shardId);
                  break;
                case HALF_OPEN_TO_OPEN:
                  log.warn(
                      "Circuit breaker for shard {} opened again after failed test calls", shardId);
                  break;
                case CLOSED_TO_FORCED_OPEN:
                  log.info("Circuit breaker for shard {} is forced open", shardId);
                  break;
              }
            })
        .onSuccess(
            event -> {
              if (breaker.getState() == CircuitBreaker.State.HALF_OPEN) {
                log.info(
                    "Test call succeeded for shard {}, failure rate: {}",
                    shardId,
                    breaker.getMetrics().getFailureRate());
              }
            })
        .onError(
            event -> {
              if (breaker.getState() == CircuitBreaker.State.HALF_OPEN) {
                log.warn(
                    "Test call failed for shard {}, failure rate: {}",
                    shardId,
                    breaker.getMetrics().getFailureRate());
              }
            });
  }

  /**
   * Disables the circuit breaker for the specified shard.
   *
   * @param shardId The shard ID
   */
  public void disableCircuitBreaker(String shardId) {
    CircuitBreaker breaker = shardCircuitBreakers.get(shardId);
    if (breaker != null) {
      breaker.transitionToDisabledState();
      log.info("Circuit breaker disabled for shard {}", shardId);
    }
  }

  /**
   * Enables the circuit breaker for the specified shard.
   *
   * @param shardId The shard ID
   */
  public void enableCircuitBreaker(String shardId) {
    CircuitBreaker breaker = shardCircuitBreakers.get(shardId);
    if (breaker != null) {
      breaker.transitionToClosedState();
      log.info("Circuit breaker enabled for shard {}", shardId);
    }
  }

  /**
   * Resets the circuit breaker for the specified shard.
   *
   * @param shardId The shard ID
   */
  public void resetCircuitBreaker(String shardId) {
    CircuitBreaker breaker = shardCircuitBreakers.get(shardId);
    if (breaker != null) {
      breaker.reset();
      log.info("Circuit breaker reset for shard {}", shardId);
    }
  }
}
