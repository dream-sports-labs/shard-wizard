package com.dream11.shardwizard.constant;

import lombok.experimental.UtilityClass;

@UtilityClass
public class Constants {

  public static final String SHARD_MANAGER_CONFIG_FOLDER = "shard-manager";
  public static final int CHECK_READONLY_MODE_INTERVAL_SECONDS = 10;
  public static final int REFRESH_SHARDS_TIMEOUT_SECONDS = 10;

  public static final String SUCCESS = "SUCCESS";
  public static final String INFO = "INFO";
  public static final String ERROR = "ERROR";
  public static final String SHARD_WIZARD = "shardwizard";
  public static final String DATADOG = "datadog";
  public static final String NEWRELIC = "newrelic";
  public static final String NOOP = "noop";

  public static class Event {
    public static final String SHARD_CHANGE = "SHARD_CHANGE";
    public static final String SHARD_DOWN = "SHARD_DOWN";
    public static final String SHARD_CHANGE_SUCCESS = "SHARD_CHANGE_SUCCESS";
    public static final String SHARD_CONFIG_UPDATED = "SHARD_CONFIG_UPDATED";
    public static final String SHARD_REGISTRATION_FAILED = "SHARD_REGISTRATION_FAILED";
    public static final String SHARD_INSERTION_FAILED = "SHARD_INSERTION_FAILED";
    public static final String CIRCUIT_BREAKER_OPEN = "CIRCUIT_BREAKER_OPEN";
    public static final String ENTITY_SHARD_MAPPING_NOT_FOUND = "ENTITY_SHARD_MAPPING_NOT_FOUND";
  }

  public static class Metric {
    public static final String DB_CONNECT = "DB_CONNECT";
    public static final String DB_CLOSE = "DB_CLOSE";
    public static final String DB_QUERY = "DB_QUERY";
    public static final String DB_BATCH_QUERY = "DB_BATCH_QUERY";
    public static final String DB_RAW_QUERY = "DB_RAW_QUERY";
    public static final String DB_QUERY_READ = "DB_QUERY_READ";
    public static final String DB_QUERY_MODIFY = "DB_QUERY_MODIFY";
    public static final String DB_BATCH_QUERY_MODIFY = "DB_BATCH_QUERY_MODIFY";
  }
}
