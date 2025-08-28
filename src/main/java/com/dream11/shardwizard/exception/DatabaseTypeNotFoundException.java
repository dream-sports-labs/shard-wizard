package com.dream11.shardwizard.exception;

public class DatabaseTypeNotFoundException extends RuntimeException {

  public DatabaseTypeNotFoundException(String databaseType) {
    super("Database type " + databaseType + " not found");
  }

  public DatabaseTypeNotFoundException() {}
}
