package com.dream11.shardwizard.circuitbreaker.exception;

/** Exception thrown when a circuit breaker is in OPEN state and rejects the operation. */
public class CircuitBreakerOpenException extends RuntimeException {
  public CircuitBreakerOpenException(String message) {
    super(message);
  }
}
