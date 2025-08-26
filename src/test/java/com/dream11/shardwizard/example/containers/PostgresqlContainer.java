package com.dream11.shardwizard.example.containers;

import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresqlContainer {

  public static PostgreSQLContainer<?> create(
      String userName,
      String password,
      String databaseName,
      int exposedPort,
      String initScriptPath) {
    int defaultPort = 5432;
    PostgreSQLContainer<?> postgresContainer =
        new PostgreSQLContainer<>("postgres:latest")
            .withDatabaseName(databaseName)
            .withUsername(userName)
            .withPassword(password)
            .withExposedPorts(defaultPort)
            .withInitScript(initScriptPath);

    postgresContainer.setPortBindings(
        java.util.Collections.singletonList("" + exposedPort + ":" + defaultPort));

    postgresContainer.start();

    // Print connection details
    System.out.println("Postgres container is running!");
    System.out.println("JDBC URL: " + postgresContainer.getJdbcUrl());
    System.out.println("Username: " + postgresContainer.getUsername());
    System.out.println("Password: " + postgresContainer.getPassword());
    System.out.println("Mapped Port: " + postgresContainer.getMappedPort(defaultPort));
    return postgresContainer;
  }
}
