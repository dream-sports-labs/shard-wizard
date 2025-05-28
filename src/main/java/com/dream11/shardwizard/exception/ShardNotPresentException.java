package com.dream11.shardwizard.exception;

public class ShardNotPresentException extends RuntimeException {

  public ShardNotPresentException(long shardId) {
    super("Shard " + shardId + " is not present");
  }
}
