package com.dream11.shardwizard.example.runs.shardmanager;

import static com.dream11.shardwizard.example.BaseShardTest.DEFAULT_ACCESS_KEY;
import static com.dream11.shardwizard.example.BaseShardTest.DEFAULT_SECRET_KEY;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.containers.DynamoContainer;
import com.dream11.shardwizard.example.containers.LocalStackS3Container;
import com.dream11.shardwizard.example.containers.MysqlContainer;
import com.dream11.shardwizard.example.containers.PostgresqlContainer;
import com.dream11.shardwizard.example.dto.DBShardConfigDTO;
import com.dream11.shardwizard.example.utils.DynamoContainerUtils;
import com.dream11.shardwizard.example.utils.S3ContainerUtils;
import java.io.File;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.regions.Region;

@Slf4j
public class ShardManagerTestSetup {
  private static final String POSTGRES_CONFIG_PATH =
      "src/test/resources/sql/postgres/postgres-shard-setup.json";
  private static final String MYSQL_CONFIG_PATH =
      "src/test/resources/sql/mysql/mysql-shard-setup.json";
  private static final String DYNAMO_CONFIG_PATH =
      "src/test/resources/dynamo/dynamo-shard-setup.json";
  private static final ObjectMapper mapper = new ObjectMapper();

  private static DBShardConfigDTO postgresConfig;
  private static DBShardConfigDTO mysqlConfig;
  private static DBShardConfigDTO dynamoConfig;

  public static void setup(String sourceType) throws Exception {
    log.info("Setting up test environment");
    loadConfigurations();
    setupEnvironment(sourceType);
    log.info("Test environment setup completed");
  }

  private static void loadConfigurations() throws Exception {
    postgresConfig = mapper.readValue(new File(POSTGRES_CONFIG_PATH), DBShardConfigDTO.class);
    mysqlConfig = mapper.readValue(new File(MYSQL_CONFIG_PATH), DBShardConfigDTO.class);
    dynamoConfig = mapper.readValue(new File(DYNAMO_CONFIG_PATH), DBShardConfigDTO.class);
  }

  private static void setupEnvironment(String sourceType) throws Exception {
    switch (DatabaseType.valueOf(sourceType)) {
      case POSTGRES:
        setupPostgresContainers();
        break;
      case MYSQL:
        setupMysqlContainers();
        break;
      case S3:
        setupS3Environment();
        break;
      case DYNAMO:
        setupDynamoEnvironment();
        break;
      default:
        log.warn("Unknown source type: {}", sourceType);
    }
  }

  private static void setupPostgresContainers() {
    log.info("Setting up PostgreSQL containers");
    PostgresqlContainer.create(
        postgresConfig.getUserName(),
        postgresConfig.getPassword(),
        postgresConfig.getDbName(),
        postgresConfig.getShardManager().getPort(),
        postgresConfig.getShardManager().getInitScript());
  }

  private static void setupMysqlContainers() {
    log.info("Setting up MySQL containers");
    MysqlContainer.create(
        mysqlConfig.getUserName(),
        mysqlConfig.getPassword(),
        mysqlConfig.getDbName(),
        mysqlConfig.getShardManager().getPort(),
        mysqlConfig.getShardManager().getInitScript());
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

  private static void setupDynamoEnvironment() throws Exception {
    GenericContainer<?> dynamoContainer =
        DynamoContainer.create(dynamoConfig.getShardManager().getPort());
    String endpoint = DynamoContainer.getEndpoint(dynamoContainer, 8000);
    DynamoContainerUtils dynamoContainerUtils = new DynamoContainerUtils();
    DynamoContainerUtils.DynamoConfig config =
        DynamoContainerUtils.DynamoConfig.builder()
            .accessKey(DEFAULT_ACCESS_KEY)
            .secretKey(DEFAULT_SECRET_KEY)
            .endpoint(endpoint)
            .region(Region.US_EAST_1.toString())
            .build();

    dynamoContainerUtils.initializeTestEnvironment(config);
  }

  // PostgreSQL configuration getters
  public static String getPostgresDb() {
    return postgresConfig.getDbName();
  }

  public static String getPostgresUser() {
    return postgresConfig.getUserName();
  }

  public static String getPostgresPassword() {
    return postgresConfig.getPassword();
  }

  public static Integer getPostgresPort() {
    return postgresConfig.getShardManager().getPort();
  }

  // MySQL configuration getters
  public static String getMysqlDb() {
    return mysqlConfig.getDbName();
  }

  public static String getMysqlUser() {
    return mysqlConfig.getUserName();
  }

  public static String getMysqlPassword() {
    return mysqlConfig.getPassword();
  }

  public static Integer getMysqlPort() {
    return mysqlConfig.getShardManager().getPort();
  }
}
