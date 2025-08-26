package com.dream11.shardwizard.circuitbreaker.manager;

import com.dream11.shardwizard.circuitbreaker.exception.CircuitBreakerOpenException;
import com.dream11.shardwizard.constant.Constants;
import com.dream11.shardwizard.dto.ObservabilityEvent;
import com.dream11.shardwizard.metric.ObservabilityServiceFactory;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;

/**
 * Utility class for circuit breaker operations. Provides methods to wrap reactive operations with
 * circuit breaker functionality.
 */
@Slf4j
public class CircuitBreakerUtils {

  /**
   * Wraps a Single operation with circuit breaker functionality.
   *
   * @param shardId The shard ID for logging purposes
   * @param breaker The circuit breaker instance
   * @param operation The operation to wrap
   * @param <T> The type of the operation result
   * @return Single wrapped with circuit breaker
   */
  public static <T> Single<T> withCircuitBreakerSingle(
      String shardId, CircuitBreaker breaker, Supplier<Single<T>> operation) {
    if (breaker.tryAcquirePermission()) {
      long startTime = System.nanoTime();
      return operation
          .get()
          .doOnSuccess(
              result -> {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                breaker.onSuccess(duration, TimeUnit.MILLISECONDS);
              })
          .doOnError(
              error -> {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                log.info("Error Class type: {}", error.getClass().getName());
                log.info("Error occurred for shard {}: {}", shardId, error.getMessage());
                breaker.onError(duration, TimeUnit.MILLISECONDS, error);
              });
    } else {
      String message = String.format("Circuit breaker is open for shard %s", shardId);
      ObservabilityEvent observabilityEvent =
          ObservabilityEvent.builder()
              .title(Constants.Event.CIRCUIT_BREAKER_OPEN)
              .description(message)
              .severity(ObservabilityEvent.EventSeverity.ERROR)
              .tags(Map.of("shardId", shardId))
              .build();
      ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
      return Single.error(new CircuitBreakerOpenException(message));
    }
  }

  /**
   * Wraps a Completable operation with circuit breaker functionality.
   *
   * @param shardId The shard ID for logging purposes
   * @param breaker The circuit breaker instance
   * @param operation The operation to wrap
   * @return Completable wrapped with circuit breaker
   */
  public static Completable withCircuitBreakerCompletable(
      String shardId, CircuitBreaker breaker, Supplier<Completable> operation) {
    if (breaker.tryAcquirePermission()) {
      long startTime = System.nanoTime();
      return operation
          .get()
          .doOnComplete(
              () -> {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                breaker.onSuccess(duration, TimeUnit.MILLISECONDS);
              })
          .doOnError(
              error -> {
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime);
                if (error instanceof java.util.concurrent.TimeoutException) {
                  breaker.onError(duration, TimeUnit.MILLISECONDS, error);
                  log.error("Operation timed out for shard {}: {}", shardId, error.getMessage());
                } else {
                  log.error("Operation failed for shard {}: {}", shardId, error.getMessage());
                  breaker.onError(duration, TimeUnit.MILLISECONDS, error);
                }
              });
    } else {
      String message = String.format("Circuit breaker is open for shard %s", shardId);
      ObservabilityEvent observabilityEvent =
          ObservabilityEvent.builder()
              .title(Constants.Event.CIRCUIT_BREAKER_OPEN)
              .description(message)
              .severity(ObservabilityEvent.EventSeverity.ERROR)
              .tags(Map.of("shardId", shardId))
              .build();
      ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
      return Completable.error(new CircuitBreakerOpenException(message));
    }
  }
}
