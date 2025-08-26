package com.dream11.shardwizard.example.runs.shardmanager.s3;

import static org.junit.Assert.*;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.runs.shardmanager.ShardManagerTestSetup;
import com.dream11.shardwizard.example.runs.shardmanager.utils.ConfigUpdater;
import com.dream11.shardwizard.example.utils.S3ContainerUtils;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

@Slf4j
public class ShardManagerS3Test {
  private static final String CONFIG_FILE_PATH = "config/shard-manager/default.conf";
  private static final String RESOURCES_FOLDER_PATH = "src/test/resources/";
  private static final String DB_TYPE = DatabaseType.S3.name();
  private static final String SOURCE_TYPE;
  private static final String SHARD_MASTER_PATH = "orders_shard_manager/shardmaster.json";
  private static final String BUCKET_NAME = "d11-contest-orders-load";
  private static final String HOST = "localhost";
  private static final int MAX_CONNECTIONS = 5;
  private static final int CONNECTION_TIMEOUT_MS = 500;
  private static final int MAX_WAIT_QUEUE_SIZE = 50;
  private static final long TEST_SHARD_ID = 1L;
  private static final String SHARD_ID_FIELD = "shardId";
  private static final String DETAILS_FIELD = "details";
  private static final String DATABASE_TYPE_FIELD = "databaseType";
  private static final String CONNECTION_PARAMS_FIELD = "shardConnectionParams";
  private static final String DATABASE_FIELD = "database";
  private static final String PORT_FIELD = "port";
  private static final String USERNAME_FIELD = "username";
  private static final String WRITER_HOST_FIELD = "writerHost";
  private static final String READER_HOST_FIELD = "readerHost";

  private ShardManagerClient s3Client;

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
  }

  @After
  public void tearDown() throws Exception {
    if (s3Client != null) s3Client.rxClose();
  }

  private void initializeClient() {
    s3Client = ShardManagerClient.create(Vertx.vertx());
    s3Client.rxBootstrap();
  }

  @Test
  public void testUpdateExistingShardDetails_S3_Success() {
    testUpdateExistingShardDetails(s3Client, DatabaseType.POSTGRES, this::verifyS3DatabaseUpdate);
  }

  private void testUpdateExistingShardDetails(
      ShardManagerClient client, DatabaseType dbType, DatabaseVerifier verifier) {
    ShardConfig newConfig = createTestShardConfig(dbType);

    ShardUpdateResponse response =
        client.rxUpdateExistingShardDetails(TEST_SHARD_ID, newConfig).blockingGet();

    verifyUpdateResponse(response, TEST_SHARD_ID, newConfig);
    verifier.verify(TEST_SHARD_ID, newConfig);
  }

  private void verifyS3DatabaseUpdate(long shardId, ShardConfig newConfig) {
    S3AsyncClient s3AsyncClient = S3ContainerUtils.createDefaultClient();
    String content = getS3Content(s3AsyncClient);
    verifyS3Content(content, shardId, newConfig);
  }

  private void verifyUpdateResponse(
      ShardUpdateResponse response, long shardId, ShardConfig newConfig) {
    assertEquals("Shard ID should match", shardId, response.getCurrentShardDetails().getShardId());
    assertEquals("Shard ID should match", shardId, response.getUpdatedShardDetails().getShardId());
    assertEquals(
        "New config should match", newConfig, response.getUpdatedShardDetails().getShardConfig());
    assertNotEquals(
        "Config should be different after update",
        response.getCurrentShardDetails().getShardConfig(),
        response.getUpdatedShardDetails().getShardConfig());
  }

  private String getS3Content(S3AsyncClient s3AsyncClient) {
    return s3AsyncClient
        .getObject(
            GetObjectRequest.builder().bucket(BUCKET_NAME).key(SHARD_MASTER_PATH).build(),
            AsyncResponseTransformer.toBytes())
        .thenApply(resp -> resp.asUtf8String())
        .join();
  }

  private void verifyS3Content(String content, long shardId, ShardConfig newConfig) {
    JsonArray shards = new JsonArray(content);
    boolean foundUpdatedShard = false;

    for (int i = 0; i < shards.size(); i++) {
      JsonObject shard = shards.getJsonObject(i);
      if (shard.getLong(SHARD_ID_FIELD) == shardId) {
        foundUpdatedShard = true;
        verifyS3ShardConfig(shard, newConfig);
        break;
      }
    }

    assertTrue("Updated shard should be found in S3", foundUpdatedShard);
  }

  private void verifyS3ShardConfig(JsonObject shard, ShardConfig newConfig) {
    JsonObject details = shard.getJsonObject(DETAILS_FIELD);
    JsonObject connectionParams = details.getJsonObject(CONNECTION_PARAMS_FIELD);

    assertEquals(
        "Database type should match",
        newConfig.getDatabaseType().name(),
        details.getString(DATABASE_TYPE_FIELD));
    assertEquals(
        "Database should match",
        newConfig.getShardConnectionParams().getDatabase(),
        connectionParams.getString(DATABASE_FIELD));
    assertEquals(
        "Port should match",
        newConfig.getShardConnectionParams().getPort(),
        connectionParams.getInteger(PORT_FIELD));
    assertEquals(
        "Username should match",
        newConfig.getShardConnectionParams().getUsername(),
        connectionParams.getString(USERNAME_FIELD));
    assertEquals(
        "Writer host should match",
        newConfig.getShardConnectionParams().getWriterHost(),
        connectionParams.getString(WRITER_HOST_FIELD));
    assertEquals(
        "Reader host should match",
        newConfig.getShardConnectionParams().getReaderHost(),
        connectionParams.getString(READER_HOST_FIELD));
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

  @FunctionalInterface
  private interface DatabaseVerifier {
    void verify(long shardId, ShardConfig newConfig);
  }
}
