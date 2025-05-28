package com.dream11.shardwizard.example;

import com.dream11.shardwizard.example.containers.LocalStackS3Container;
import com.dream11.shardwizard.example.containers.PostgresqlContainer;
import com.dream11.shardwizard.example.utils.S3ContainerUtils;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;

@Slf4j
public class StandardIntegrationTestSetup {

  private static final String SQL_PATH = "sql/postgres/";
  private static final String POSTGRES_USER = "postgres";
  private static final String POSTGRES_PASSWORD = "postgres";
  private static final String POSTGRES_DB = "postgres";

  public static void setup() throws Exception {
    log.info("Setting up test environment");
    setupPostgresContainers();
    setupS3Environment();
    log.info("Test environment setup completed");
  }

  private static void setupPostgresContainers() {
    log.info("Setting up PostgreSQL containers");

    // Shard manager database
    PostgreSQLContainer shardManager =
        PostgresqlContainer.create(
            POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, 5400, "sql/shard-manager.sql");

    // Shard databases
    for (int i = 1; i <= 5; i++) {
      int port = 5432 + i;
      PostgresqlContainer.create(
          POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, port, SQL_PATH + "postgres-shard.sql");
      log.info("Created shard {} on port {}", i, port);
    }
  }

  private static void setupS3Environment() throws Exception {
    log.info("Setting up S3 environment");

    LocalStackContainer localStack = LocalStackS3Container.create();
    S3ContainerUtils s3ContainerUtils = new S3ContainerUtils();
    S3ContainerUtils.S3Config config =
        S3ContainerUtils.S3Config.builder()
            .accessKey(localStack.getAccessKey())
            .secretKey(localStack.getSecretKey())
            .region(localStack.getRegion())
            .endpoint(localStack.getEndpoint())
            .bucketName("d11-contest-orders-load")
            .baseFolder("orders_shard_manager")
            .build();

    s3ContainerUtils.initializeTestEnvironment(config);
  }
}
