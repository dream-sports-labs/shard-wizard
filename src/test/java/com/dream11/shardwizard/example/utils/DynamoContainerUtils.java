package com.dream11.shardwizard.example.utils;

import com.dream11.shardwizard.constant.DatabaseType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Reader;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

@Slf4j
public class DynamoContainerUtils {
  private static final String LOCAL_ENDPOINT = "http://localhost:8999";
  private static final String DEFAULT_REGION = Region.US_EAST_1.toString();

  private final DynamoDbClient dynamoDbClient;

  @Builder
  @Data
  public static class DynamoConfig {
    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final String endpoint;
    private final String tableName;
  }

  public DynamoContainerUtils() {
    this.dynamoDbClient = createDefaultClient();
  }

  public static DynamoDbClient createDefaultClient() {
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCAL_ENDPOINT))
        .region(Region.of(DEFAULT_REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("dummy", "dummy")))
        .build();
  }

  public void initializeTestEnvironment(DynamoConfig config) throws Exception {
    log.info("Initializing DynamoDB test environment with config: {}", config);

    createTables();
    insertShardMasterData();
    insertEntityShardMappingFromCSV("src/test/resources/dynamo/entity_shard_mapping.csv");
  }

  private void createTables() {
    // Create ShardMaster Table
    dynamoDbClient.createTable(
        CreateTableRequest.builder()
            .tableName("ShardMaster")
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("shard_id")
                    .attributeType(ScalarAttributeType.N)
                    .build())
            .keySchema(
                KeySchemaElement.builder().attributeName("shard_id").keyType(KeyType.HASH).build())
            .provisionedThroughput(defaultThroughput())
            .build());

    // Create EntityShardMapping Table
    dynamoDbClient.createTable(
        CreateTableRequest.builder()
            .tableName("EntityShardMapping")
            .attributeDefinitions(
                AttributeDefinition.builder()
                    .attributeName("entity_id")
                    .attributeType(ScalarAttributeType.S)
                    .build())
            .keySchema(
                KeySchemaElement.builder().attributeName("entity_id").keyType(KeyType.HASH).build())
            .provisionedThroughput(defaultThroughput())
            .build());
  }

  public static ProvisionedThroughput defaultThroughput() {
    return ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L).build();
  }

  private void insertShardMasterData() {

    for (Map<String, Object> data :
        readShardsFromJson("src/test/resources/dynamo/shard_master.json")) {
      dynamoDbClient.putItem(
          PutItemRequest.builder().tableName("ShardMaster").item(toItem(data)).build());
    }

    System.out.println("✅ ShardMaster data inserted.");
  }

  private List<Map<String, Object>> readShardsFromJson(String path) {
    ObjectMapper mapper = new ObjectMapper();
    try (Reader reader = Files.newBufferedReader(Paths.get(path))) {
      JsonNode root = mapper.readTree(reader);
      List<Map<String, Object>> shards = new ArrayList<>();

      for (JsonNode node : root) {
        int shardId = node.get("shardId").asInt();
        boolean isDefault = node.get("default").asBoolean();
        boolean isActive = node.get("active").asBoolean();

        String databaseType = node.get("details").get("databaseType").asText();
        int port = node.get("details").get("shardConnectionParams").get("port").asInt();

        Map<String, Object> shard =
            DatabaseType.DYNAMO.name().equalsIgnoreCase(databaseType)
                ? shard(shardId, isDefault, isActive, port, DatabaseType.DYNAMO.name())
                : shard(shardId, isActive, isDefault, port);

        shards.add(shard);
      }
      return shards;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read shards.json", e);
    }
  }

  private Map<String, Object> shard(int id, boolean def, boolean active, int port, String dbType) {
    return Map.of(
        "shard_id",
        id,
        "is_active",
        active,
        "is_default",
        def,
        "details",
        Map.of(
            "databaseType",
            dbType,
            "shardConnectionParams",
            Map.of(
                "port",
                port,
                "endpoint",
                "http://localhost:" + port,
                "region",
                "us-east-1",
                "accessKey",
                "dummy",
                "secretKey",
                "dummy",
                "readerHost",
                "localhost",
                "writerHost",
                "localhost",
                "maxConnections",
                5,
                "maxWaitQueueSize",
                50,
                "connectionTimeoutMs",
                500)),
        "created_at",
        Instant.now().toString(),
        "updated_at",
        Instant.now().toString());
  }

  private Map<String, Object> shard(int id, boolean active, boolean def, int port) {
    return shard(id, def, active, port, DatabaseType.POSTGRES.name());
  }

  private static Map<String, AttributeValue> toItem(Map<String, Object> data) {
    Map<String, AttributeValue> item = new HashMap<>();
    data.forEach((k, v) -> item.put(k, wrap(v)));
    return item;
  }

  private static AttributeValue wrap(Object val) {
    if (val instanceof String) return s((String) val);
    if (val instanceof Boolean) return AttributeValue.builder().bool((Boolean) val).build();
    if (val instanceof Number) return n(val.toString());
    if (val instanceof List) {
      List<?> list = (List<?>) val;
      return l(list.stream().map(DynamoContainerUtils::wrap).toArray(AttributeValue[]::new));
    }
    if (val instanceof Map) {
      Map<?, ?> rawMap = (Map<?, ?>) val;
      Map<String, AttributeValue> nested = new HashMap<>();
      rawMap.forEach((k, v) -> nested.put(k.toString(), wrap(v)));
      return AttributeValue.builder().m(nested).build();
    }
    throw new IllegalArgumentException("Unsupported type: " + val);
  }

  private static AttributeValue s(String val) {
    return AttributeValue.builder().s(val).build();
  }

  private static AttributeValue n(String val) {
    return AttributeValue.builder().n(val).build();
  }

  private static AttributeValue l(AttributeValue... values) {
    return AttributeValue.builder().l(values).build();
  }

  private void insertEntityShardMappingFromCSV(String csvPath) throws IOException {
    try (Reader reader = Files.newBufferedReader(Paths.get(csvPath))) {
      Iterable<CSVRecord> records = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);

      for (CSVRecord record : records) {
        String entityId = record.get("entity_id");
        List<AttributeValue> shardIds =
            Arrays.stream(record.get("shard_ids").split(","))
                .map(String::trim)
                .map(id -> AttributeValue.builder().n(id).build())
                .collect(Collectors.toList());

        dynamoDbClient.putItem(
            PutItemRequest.builder()
                .tableName("EntityShardMapping")
                .item(
                    Map.of(
                        "entity_id", s(entityId),
                        "shard_ids", AttributeValue.builder().l(shardIds).build(),
                        "created_at", s(Instant.now().toString()),
                        "updated_at", s(Instant.now().toString())))
                .build());
      }
    }

    System.out.println("✅ EntityShardMapping seeded from CSV.");
  }
}
