package com.dream11.shardwizard.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.typesafe.config.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class CircuitBreakerConfigDTO {

  /**
   * Flag to disable circuit breaker functionality. When set to true: - No circuit breaker instance
   * will be created - Operations will bypass the circuit breaker wrapper - All calls will be
   * executed directly without circuit breaker protection
   */
  @Builder.Default private Boolean enabled = false;

  /**
   * The failure rate threshold in percentage. When the failure rate is equal or greater than this
   * threshold, the CircuitBreaker transitions to open and starts short-circuiting calls. Default:
   * 50
   */
  @Optional private Integer failureRateThreshold;

  /**
   * The wait duration in milliseconds which specifies how long the CircuitBreaker should stay open,
   * before it switches to half-open. Default: 60000 (60 seconds)
   */
  @Optional private Integer waitDurationInOpenState;

  /**
   * The size of the sliding window which is used to record the outcome of calls when the
   * CircuitBreaker is closed. For COUNT_BASED sliding window: Default: 100 For TIME_BASED sliding
   * window: Default: 60 (seconds)
   */
  @Optional private Integer slidingWindowSize;

  /**
   * The type of the sliding window. Can be either "COUNT_BASED" or "TIME_BASED". COUNT_BASED: The
   * sliding window is based on the number of calls. TIME_BASED: The sliding window is based on time
   * duration. Default: COUNT_BASED
   */
  @Optional private String slidingWindowType;

  /**
   * The number of permitted calls when the CircuitBreaker is half-open. These calls are used to
   * determine if the CircuitBreaker should transition to closed or open state. Default: 10
   */
  @Optional private Integer permittedNumberOfCallsInHalfOpenState;

  /**
   * The minimum number of calls which are required (per sliding window period) before the
   * CircuitBreaker can calculate the error rate or slow call rate. Default: 100
   */
  @Optional private Integer minimumNumberOfCalls;

  /**
   * The slow call rate threshold in percentage. When the percentage of slow calls is equal or
   * greater than this threshold, the CircuitBreaker transitions to open and starts short-circuiting
   * calls. Default: 100
   */
  @Optional private Integer slowCallRateThreshold;

  /**
   * The duration threshold in milliseconds. Calls that take longer than this threshold are
   * considered as slow calls. Default: 60000 (60 seconds)
   */
  @Optional private Integer slowCallDurationThreshold;
}
