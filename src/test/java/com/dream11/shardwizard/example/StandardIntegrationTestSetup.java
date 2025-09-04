package com.dream11.shardwizard.example;

import static com.dream11.shardwizard.example.ShardTestSupport.DEFAULT_ACCESS_KEY;
import static com.dream11.shardwizard.example.ShardTestSupport.DEFAULT_SECRET_KEY;
import static com.dream11.shardwizard.example.utils.Constants.TABLE_NAME;
import static com.dream11.shardwizard.example.utils.DynamoContainerUtils.defaultThroughput;

import com.dream11.shardwizard.example.containers.DynamoContainer;
import com.dream11.shardwizard.example.containers.LocalStackS3Container;
import com.dream11.shardwizard.example.containers.MysqlContainer;
import com.dream11.shardwizard.example.containers.PostgresqlContainer;
import com.dream11.shardwizard.example.dto.DBShardConfigDTO;
import com.dream11.shardwizard.example.utils.DynamoContainerUtils;
import com.dream11.shardwizard.example.utils.S3ContainerUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@Slf4j
public class StandardIntegrationTestSetup {
  private static DBShardConfigDTO postgresConfig;
  private static DBShardConfigDTO mysqlConfig;
  private static DBShardConfigDTO dynamoConfig;

  public static void setup() throws Exception {
    log.info("Setting up test environment");
    setupPostgresContainers();
    setupMysqlContainers();
    setupS3Environment();
    setupDynamoContainers();
    log.info("Test environment setup completed");
  }

  public static void setupPostgresContainers() throws IOException {
    log.info("Setting up Postgres containers");

    ObjectMapper mapper = new ObjectMapper();
    postgresConfig =
        mapper.readValue(
            new File("src/test/resources/sql/postgres/postgres-shard-setup.json"),
            DBShardConfigDTO.class);

    PostgresqlContainer.create(
        postgresConfig.getUserName(),
        postgresConfig.getPassword(),
        postgresConfig.getDbName(),
        postgresConfig.getShardManager().getPort(),
        postgresConfig.getShardManager().getInitScript());

    for (DBShardConfigDTO.Shard shard : postgresConfig.getShards()) {
      PostgresqlContainer.create(
          postgresConfig.getUserName(),
          postgresConfig.getPassword(),
          postgresConfig.getDbName(),
          shard.getPort(),
          shard.getInitScript());
      log.info("Created Postgres shard on port {}", shard.getPort());
    }
  }

  public static void setupMysqlContainers() throws IOException {
    log.info("Setting up MySQL containers");

    ObjectMapper mapper = new ObjectMapper();

    mysqlConfig =
        mapper.readValue(
            new File("src/test/resources/sql/mysql/mysql-shard-setup.json"),
            DBShardConfigDTO.class);

    MysqlContainer.create(
        mysqlConfig.getUserName(),
        mysqlConfig.getPassword(),
        mysqlConfig.getDbName(),
        mysqlConfig.getShardManager().getPort(),
        mysqlConfig.getShardManager().getInitScript());

    for (DBShardConfigDTO.Shard shard : mysqlConfig.getShards()) {
      MysqlContainer.create(
          mysqlConfig.getUserName(),
          mysqlConfig.getPassword(),
          mysqlConfig.getDbName(),
          shard.getPort(),
          shard.getInitScript());
      log.info("Created MySQL shard on port {}", shard.getPort());
    }
  }

  public static void setupDynamoContainers() throws Exception {

    ObjectMapper mapper = new ObjectMapper();

    dynamoConfig =
        mapper.readValue(
            new File("src/test/resources/dynamo/dynamo-shard-setup.json"), DBShardConfigDTO.class);

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

    // Create containers for shards
    for (DBShardConfigDTO.Shard shard : dynamoConfig.getShards()) {
      GenericContainer<?> shardContainer = DynamoContainer.create(shard.getPort());
      String shardEndpoint = DynamoContainer.getEndpoint(shardContainer, 8000);
      DynamoDbClient shardClient =
          DynamoDbClient.builder()
              .endpointOverride(URI.create(shardEndpoint))
              .region(Region.US_EAST_1)
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)))
              .build();

      // Create Orders Table in each shard
      CreateTableRequest createOrdersTableRequest =
          CreateTableRequest.builder()
              .tableName(TABLE_NAME)
              .attributeDefinitions(
                  AttributeDefinition.builder()
                      .attributeName("order_id")
                      .attributeType(ScalarAttributeType.S)
                      .build())
              .keySchema(
                  KeySchemaElement.builder()
                      .attributeName("order_id")
                      .keyType(KeyType.HASH)
                      .build())
              .provisionedThroughput(defaultThroughput())
              .build();

      shardClient.createTable(createOrdersTableRequest);
      log.info("Created Orders table in dynamo shard on port {}", shard.getPort());
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
