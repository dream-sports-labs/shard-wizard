package com.dream11.shardwizard.circuitbreaker.client;

import com.dream11.shardwizard.circuitbreaker.CircuitBreakerEnabledClient;
import com.dream11.shardwizard.circuitbreaker.manager.CircuitBreakerManager;
import com.dream11.shardwizard.model.CircuitBreakerConfigDTO;
import com.dream11.shardwizard.model.ShardDetails;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;

/**
 * Abstract base class that implements CircuitBreakerEnabledClient and provides common circuit
 * breaker functionality. All database clients should extend this class to get circuit breaker
 * functionality.
 */
public abstract class AbstractCircuitBreakerClient implements CircuitBreakerEnabledClient {

  protected final CircuitBreaker circuitBreaker;

  protected final String shardId;

  protected AbstractCircuitBreakerClient(ShardDetails shardDetails) {
    CircuitBreakerConfigDTO config =
        shardDetails.getShardConfig().getShardConnectionParams().getCircuitBreaker();
    this.shardId = String.valueOf(shardDetails.getShardId());
    this.circuitBreaker =
        CircuitBreakerManager.getInstance().getCircuitBreaker(this.shardId, config);
  }

  @Override
  public CircuitBreaker getCircuitBreaker() {
    return this.circuitBreaker;
  }

  @Override
  public String getShardId() {
    return this.shardId;
  }
}
