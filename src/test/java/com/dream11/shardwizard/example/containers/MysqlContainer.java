package com.dream11.shardwizard.example.containers;

import java.util.Collections;
import org.testcontainers.containers.MySQLContainer;

public class MysqlContainer {

  public static MySQLContainer<?> create(
      String userName,
      String password,
      String databaseName,
      int exposedPort,
      String initScriptPath) {
    int defaultPort = 3306;

    MySQLContainer<?> mysqlContainer =
        new MySQLContainer<>("mysql:8.0")
            .withDatabaseName(databaseName)
            .withUsername(userName)
            .withPassword(password)
            .withAccessToHost(true)
            .withInitScript(initScriptPath)
            .withExposedPorts(exposedPort);

    mysqlContainer.setPortBindings(Collections.singletonList(exposedPort + ":" + defaultPort));
    mysqlContainer.start();

    System.out.println("MySQL container is running!");
    System.out.println("JDBC URL: " + mysqlContainer.getJdbcUrl());
    System.out.println("Username: " + mysqlContainer.getUsername());
    System.out.println("Password: " + mysqlContainer.getPassword());
    System.out.println("Mapped Port: " + mysqlContainer.getMappedPort(defaultPort));

    return mysqlContainer;
  }
}
