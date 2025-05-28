package com.dream11.shardwizard.exception;

public class EntityNotMappedToShardException extends RuntimeException {

  public EntityNotMappedToShardException(String entityId) {
    super("Entity " + entityId + " is not mapped to any shard");
  }
}
