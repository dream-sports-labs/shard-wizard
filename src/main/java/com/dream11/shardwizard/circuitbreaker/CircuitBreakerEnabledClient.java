package com.dream11.shardwizard.circuitbreaker;

import com.dream11.shardwizard.circuitbreaker.manager.CircuitBreakerUtils;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.reactivex.Completable;
import io.reactivex.Single;

/**
 * Interface that defines the contract for database clients that implement circuit breaker pattern.
 * All database clients should implement this interface to ensure circuit breaker functionality.
 */
public interface CircuitBreakerEnabledClient {

  /**
   * Get the circuit breaker instance for this client
   *
   * @return CircuitBreaker instance
   */
  CircuitBreaker getCircuitBreaker();

  /**
   * Get the shard ID associated with this client
   *
   * @return shard ID as string
   */
  String getShardId();

  /**
   * Wrap a Single operation with circuit breaker
   *
   * @param operation The operation to wrap
   * @param <T> The type of the operation result
   * @return Single wrapped with circuit breaker
   */
  default <T> Single<T> withCircuitBreaker(Single<T> operation) {
    CircuitBreaker breaker = getCircuitBreaker();
    if (breaker == null) {
      return operation;
    }
    return CircuitBreakerUtils.withCircuitBreakerSingle(getShardId(), breaker, () -> operation);
  }

  /**
   * Wrap a Completable operation with circuit breaker
   *
   * @param operation The operation to wrap
   * @return Completable wrapped with circuit breaker
   */
  default Completable withCircuitBreaker(Completable operation) {
    CircuitBreaker breaker = getCircuitBreaker();
    if (breaker == null) {
      return operation;
    }
    return CircuitBreakerUtils.withCircuitBreakerCompletable(
        getShardId(), breaker, () -> operation);
  }
}
