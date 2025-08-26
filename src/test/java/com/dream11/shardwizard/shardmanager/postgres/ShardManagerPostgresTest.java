package com.dream11.shardwizard.shardmanager.postgres;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.dream11.shardwizard.shardmanager.ShardManagerTestSetup;
import com.dream11.shardwizard.shardmanager.utils.ConfigUpdater;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.vertx.reactivex.core.Vertx;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@Slf4j
public class ShardManagerPostgresTest {
  private static final String CONFIG_FILE_PATH = "config/shard-manager/default.conf";
  private static final String RESOURCES_FOLDER_PATH = "src/test/resources/";
  private static final String DB_TYPE = DatabaseType.POSTGRES.name();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String SOURCE_TYPE;
  private static final String DB_DRIVER = "postgresql";
  private static final String HOST = "localhost";
  private static final int MAX_CONNECTIONS = 5;
  private static final int CONNECTION_TIMEOUT_MS = 500;
  private static final int MAX_WAIT_QUEUE_SIZE = 50;
  private static final long TEST_SHARD_ID = 1L;
  private static final String SHARD_MASTER_TABLE = "ShardMaster";
  private static final String SHARD_ID_COLUMN = "ShardId";
  private static final String IS_ACTIVE_COLUMN = "IsActive";
  private static final String IS_DEFAULT_COLUMN = "IsDefault";
  private static final String DETAILS_COLUMN = "Details";

  private ShardManagerClient postgresClient;
  private Connection postgresConnection;

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
  public void tearDown() throws Exception {
    if (postgresConnection != null) postgresConnection.close();
  }

  private void initializeClient() {
    postgresClient = ShardManagerClient.create(Vertx.vertx());
    postgresClient.rxBootstrap();
  }

  private void initializeDatabaseConnection() throws Exception {
    initializePostgresConnection();
  }

  private void initializePostgresConnection() throws Exception {
    Properties props = new Properties();
    props.setProperty("user", ShardManagerTestSetup.getPostgresUser());
    props.setProperty("password", ShardManagerTestSetup.getPostgresPassword());
    postgresConnection =
        createDatabaseConnection(
            DB_DRIVER,
            ShardManagerTestSetup.getPostgresPort(),
            ShardManagerTestSetup.getPostgresDb(),
            props);
  }

  private Connection createDatabaseConnection(
      String dbType, int port, String dbName, Properties props) throws Exception {
    return DriverManager.getConnection(
        String.format("jdbc:%s://%s:%d/%s", dbType, HOST, port, dbName), props);
  }

  @Test
  public void testUpdateExistingShardDetails_Postgres_Success() {
    testUpdateExistingShardDetails(
        postgresClient, DatabaseType.POSTGRES, this::verifyPostgresDatabaseUpdate);
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
    assertEquals("Shard ID should match", shardId, response.getCurrentShardDetails().getShardId());
    assertEquals("Shard ID should match", shardId, response.getUpdatedShardDetails().getShardId());
    assertEquals(
        "New config should match", newConfig, response.getUpdatedShardDetails().getShardConfig());
    assertNotEquals(
        "Config should be different after update",
        response.getCurrentShardDetails().getShardConfig(),
        response.getUpdatedShardDetails().getShardConfig());
  }

  private void verifyPostgresDatabaseUpdate(long shardId, ShardConfig newConfig) {
    verifyDatabaseUpdate(postgresConnection, shardId, newConfig);
  }

  private void verifyDatabaseUpdate(Connection connection, long shardId, ShardConfig newConfig) {
    try {
      String sql =
          String.format("SELECT * FROM %s WHERE %s = ?", SHARD_MASTER_TABLE, SHARD_ID_COLUMN);
      try (PreparedStatement stmt = connection.prepareStatement(sql)) {
        stmt.setLong(1, shardId);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            verifyDatabaseRow(rs, shardId, newConfig);
          } else {
            fail("No shard details found in database for shard ID: " + shardId);
          }
        }
      }
    } catch (Exception e) {
      fail("Failed to verify database update: " + e.getMessage());
    }
  }

  private void verifyDatabaseRow(ResultSet rs, long shardId, ShardConfig newConfig)
      throws Exception {
    assertEquals("Shard ID should match", shardId, rs.getLong(SHARD_ID_COLUMN));
    assertEquals("IsActive should be true", true, rs.getBoolean(IS_ACTIVE_COLUMN));
    assertEquals("IsDefault should be false", false, rs.getBoolean(IS_DEFAULT_COLUMN));

    String detailsJson = rs.getString(DETAILS_COLUMN);
    ShardConfig storedConfig = OBJECT_MAPPER.readValue(detailsJson, ShardConfig.class);
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
