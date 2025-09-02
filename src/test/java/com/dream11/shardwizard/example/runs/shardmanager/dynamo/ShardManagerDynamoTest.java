package com.dream11.shardwizard.example.runs.shardmanager.dynamo;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.runs.shardmanager.ShardManagerTestSetup;
import com.dream11.shardwizard.example.utils.ConfigUpdater;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import java.net.URI;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

@Slf4j
public class ShardManagerDynamoTest {
  private static final String CONFIG_FILE_PATH = "config/shard-manager/default.conf";
  private static final String RESOURCES_FOLDER_PATH = "src/test/resources/";
  private static final String DB_TYPE = DatabaseType.DYNAMO.name();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SOURCE_TYPE;
  private static final String HOST = "localhost";
  private static final int PORT = 8999;
  private static final String REGION = "us-east-1";
  private static final String ACCESS_KEY = "dummy";
  private static final String SECRET_KEY = "dummy";
  private static final int MAX_CONNECTIONS = 5;
  private static final int CONNECTION_TIMEOUT_MS = 500;
  private static final int MAX_WAIT_QUEUE_SIZE = 50;
  private static final long TEST_SHARD_ID = 1L;
  private static final String SHARD_MASTER_TABLE = "ShardMaster";
  private static final String SHARD_ID_ATTRIBUTE = "shard_id";
  private static final String IS_ACTIVE_ATTRIBUTE = "is_active";
  private static final String IS_DEFAULT_ATTRIBUTE = "is_default";
  private static final String DETAILS_ATTRIBUTE = "details";
  private static final String DATABASE_TYPE_ATTRIBUTE = "databaseType";
  private static final String CONNECTION_PARAMS_ATTRIBUTE = "shardConnectionParams";

  private ShardManagerClient dynamoClient;
  private DynamoDbClient dynamoConnection;

  static {
    try {
      // TODO: Instead of Updating, need to inject this
      ConfigUpdater.updateSourceTypeInConfigFile(RESOURCES_FOLDER_PATH + CONFIG_FILE_PATH, DB_TYPE);
      Config config = ConfigFactory.load(CONFIG_FILE_PATH);
      SOURCE_TYPE = config.getString("sourceType");
      ShardManagerTestSetup.setup(SOURCE_TYPE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Before
  public void setup() throws Exception {
    initializeClient();
    initializeDatabaseConnection();
  }

  @After
  public void tearDown() {
    if (dynamoClient != null) dynamoClient.rxClose();
  }

  private void initializeClient() {
    dynamoClient = ShardManagerClient.create(Vertx.vertx());
    dynamoClient.rxBootstrap().blockingAwait();
  }

  private void initializeDatabaseConnection() {
    dynamoConnection =
        DynamoDbClient.builder()
            .endpointOverride(URI.create(String.format("http://%s:%d", HOST, PORT)))
            .region(Region.of(REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build();
  }

  @Test
  public void testUpdateExistingShardDetails_Dynamo_Success() {
    testUpdateExistingShardDetails(
        dynamoClient, DatabaseType.POSTGRES, this::verifyDynamoDatabaseUpdate);
  }

  private void testUpdateExistingShardDetails(
      ShardManagerClient client, DatabaseType dbType, DatabaseVerifier verifier) {
    ShardConfig newConfig = createTestShardConfig(dbType);

    ShardUpdateResponse response =
        client.rxUpdateExistingShardDetails(TEST_SHARD_ID, newConfig).blockingGet();

    verifyUpdateResponse(response, TEST_SHARD_ID, newConfig);
    verifier.verify(TEST_SHARD_ID, newConfig);
  }

  private ShardConfig createTestShardConfig(DatabaseType databaseType) {
    ShardConnectionParameters connectionParams =
        ShardConnectionParameters.builder()
            .database(
                databaseType == DatabaseType.MYSQL
                    ? ShardManagerTestSetup.getMysqlDb()
                    : ShardManagerTestSetup.getPostgresDb())
            .port(
                databaseType == DatabaseType.MYSQL
                    ? ShardManagerTestSetup.getMysqlPort()
                    : ShardManagerTestSetup.getPostgresPort())
            .username(
                databaseType == DatabaseType.MYSQL
                    ? ShardManagerTestSetup.getMysqlUser()
                    : ShardManagerTestSetup.getPostgresUser())
            .password(
                databaseType == DatabaseType.MYSQL
                    ? ShardManagerTestSetup.getMysqlPassword()
                    : ShardManagerTestSetup.getPostgresPassword())
            .writerHost(HOST)
            .readerHost(HOST)
            .maxConnections(MAX_CONNECTIONS)
            .connectionTimeoutMs(CONNECTION_TIMEOUT_MS)
            .maxWaitQueueSize(MAX_WAIT_QUEUE_SIZE)
            .build();

    return ShardConfig.builder()
        .databaseType(databaseType)
        .shardConnectionParams(connectionParams)
        .build();
  }

  private void verifyUpdateResponse(
      ShardUpdateResponse response, long shardId, ShardConfig newConfig) {
    // required for Dynamo test Cases as it is responding with default value
    newConfig.getShardConnectionParams().setAccessKey("");
    newConfig.getShardConnectionParams().setSecretKey("");

    assertEquals("Shard ID should match", shardId, response.getCurrentShardDetails().getShardId());
    assertEquals("Shard ID should match", shardId, response.getUpdatedShardDetails().getShardId());
    assertEquals(
        "New config should match", newConfig, response.getUpdatedShardDetails().getShardConfig());
    assertNotEquals(
        "Config should be different after update",
        response.getCurrentShardDetails().getShardConfig(),
        response.getUpdatedShardDetails().getShardConfig());
  }

  private void verifyDynamoDatabaseUpdate(long shardId, ShardConfig newConfig) {
    verifyDynamoUpdate(dynamoConnection, shardId, newConfig);
  }

  private void verifyDynamoUpdate(
      DynamoDbClient dynamoClient, long shardId, ShardConfig newConfig) {
    try {
      log.info("Verifying DynamoDB update for shardId: {}", shardId);
      GetItemResponse response =
          dynamoClient.getItem(
              GetItemRequest.builder()
                  .tableName(SHARD_MASTER_TABLE)
                  .key(
                      Map.of(
                          SHARD_ID_ATTRIBUTE,
                          AttributeValue.builder().n(String.valueOf(shardId)).build()))
                  .build());

      if (response.hasItem()) {
        Map<String, AttributeValue> item = response.item();
        log.info("Found item in DynamoDB: {}", item);
        verifyDynamoItem(item, shardId, newConfig);
      } else {
        log.error("No item found in DynamoDB for shardId: {}", shardId);
        fail("No shard details found in DynamoDB for shard ID: " + shardId);
      }
    } catch (Exception e) {
      log.error("Failed to verify DynamoDB update", e);
      fail("Failed to verify DynamoDB update: " + e.getMessage());
    }
  }

  private void verifyDynamoItem(
      Map<String, AttributeValue> item, long shardId, ShardConfig newConfig) throws Exception {
    log.info("Verifying DynamoDB item for shardId: {}", shardId);
    assertEquals(
        "Shard ID should match", shardId, Long.parseLong(item.get(SHARD_ID_ATTRIBUTE).n()));
    assertEquals("IsActive should be true", true, item.get(IS_ACTIVE_ATTRIBUTE).bool());
    assertEquals("IsDefault should be false", false, item.get(IS_DEFAULT_ATTRIBUTE).bool());

    Map<String, AttributeValue> details = item.get(DETAILS_ATTRIBUTE).m();
    log.info("Details from DynamoDB: {}", details);

    // Convert the DynamoDB map to a JSON string
    JsonObject detailsJson = new JsonObject();
    detailsJson.put(DATABASE_TYPE_ATTRIBUTE, details.get(DATABASE_TYPE_ATTRIBUTE).s());

    JsonObject connectionParams = new JsonObject();
    Map<String, AttributeValue> params = details.get(CONNECTION_PARAMS_ATTRIBUTE).m();
    connectionParams.put("writerHost", params.get("writerHost").s());
    connectionParams.put("readerHost", params.get("readerHost").s());
    connectionParams.put("port", Integer.parseInt(params.get("port").n()));
    connectionParams.put("maxConnections", Integer.parseInt(params.get("maxConnections").n()));
    connectionParams.put("maxWaitQueueSize", Integer.parseInt(params.get("maxWaitQueueSize").n()));
    connectionParams.put(
        "connectionTimeoutMs", Integer.parseInt(params.get("connectionTimeoutMs").n()));
    connectionParams.put("username", params.get("username").s());
    connectionParams.put("password", params.get("password").s());
    connectionParams.put("database", params.get("database").s());

    detailsJson.put(CONNECTION_PARAMS_ATTRIBUTE, connectionParams);

    log.info("Converted details JSON: {}", detailsJson);
    ShardConfig storedConfig = OBJECT_MAPPER.readValue(detailsJson.toString(), ShardConfig.class);
    verifyShardConfig(newConfig, storedConfig);
  }

  private void verifyShardConfig(ShardConfig expected, ShardConfig actual) {
    assertEquals(
        "Database type should match", expected.getDatabaseType(), actual.getDatabaseType());
    assertEquals(
        "Database name should match",
        expected.getShardConnectionParams().getDatabase(),
        actual.getShardConnectionParams().getDatabase());
    assertEquals(
        "Port should match",
        expected.getShardConnectionParams().getPort(),
        actual.getShardConnectionParams().getPort());
    assertEquals(
        "Username should match",
        expected.getShardConnectionParams().getUsername(),
        actual.getShardConnectionParams().getUsername());
  }

  @FunctionalInterface
  private interface DatabaseVerifier {
    void verify(long shardId, ShardConfig newConfig);
  }
}
