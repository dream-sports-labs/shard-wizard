package com.dream11.shardwizard.example.utils;

import static com.dream11.shardwizard.example.ShardTestSupport.DEFAULT_ACCESS_KEY;
import static com.dream11.shardwizard.example.ShardTestSupport.DEFAULT_SECRET_KEY;

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
  }

  public DynamoContainerUtils() {
    this.dynamoDbClient = createDefaultClient();
  }

  public static DynamoDbClient createDefaultClient() {
    return DynamoDbClient.builder()
        .endpointOverride(URI.create(LOCAL_ENDPOINT))
        .region(Region.of(DEFAULT_REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(
                AwsBasicCredentials.create(DEFAULT_ACCESS_KEY, DEFAULT_SECRET_KEY)))
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
        int shardId = node.path("shardId").asInt();
        boolean isDefault = node.path("default").asBoolean();
        boolean isActive = node.path("active").asBoolean();

        JsonNode details = node.path("details");
        String databaseType = details.path("databaseType").asText();

        JsonNode params = details.path("shardConnectionParams");
        // Build the shard map using all params present (works for DYNAMO and others)
        shards.add(shard(shardId, isDefault, isActive, databaseType, params));
      }
      return shards;
    } catch (IOException e) {
      throw new RuntimeException("Failed to read shards.json", e);
    }
  }

  private Map<String, Object> shard(
      int id, boolean isDefault, boolean isActive, String dbType, JsonNode params) {

    // Common params (present in your sample JSON and harmless if absent)
    int port = params.path("port").asInt(0);
    String endpoint = params.path("endpoint").asText(null);
    String region = params.path("region").asText(null);
    String accessKey = params.path("accessKey").asText(null);
    String secretKey = params.path("secretKey").asText(null);
    String readerHost = params.path("readerHost").asText(null);
    String writerHost = params.path("writerHost").asText(null);
    int maxConnections = params.path("maxConnections").asInt(0);
    int maxWaitQueueSize = params.path("maxWaitQueueSize").asInt(0);
    int connectionTimeoutMs = params.path("connectionTimeoutMs").asInt(0);

    Map<String, Object> connectionParams = new LinkedHashMap<>();
    // Always include what we know; consumers can ignore nulls if not needed
    connectionParams.put("port", port);
    connectionParams.put("endpoint", endpoint);
    connectionParams.put("region", region);
    connectionParams.put("accessKey", accessKey);
    connectionParams.put("secretKey", secretKey);
    connectionParams.put("readerHost", readerHost);
    connectionParams.put("writerHost", writerHost);
    connectionParams.put("maxConnections", maxConnections);
    connectionParams.put("maxWaitQueueSize", maxWaitQueueSize);
    connectionParams.put("connectionTimeoutMs", connectionTimeoutMs);

    return Map.of(
        "shard_id",
        id,
        "is_active",
        isActive,
        "is_default",
        isDefault,
        "details",
        Map.of(
            "databaseType", dbType,
            "shardConnectionParams", connectionParams),
        "created_at",
        Instant.now().toString(),
        "updated_at",
        Instant.now().toString());
  }

  private static Map<String, AttributeValue> toItem(Map<String, Object> data) {
    Map<String, AttributeValue> item = new HashMap<>();
    data.forEach((k, v) -> item.put(k, wrap(v)));
    return item;
  }

  private static AttributeValue wrap(Object val) {
    if (val == null) {
      // represent null safely in Dynamo
      return AttributeValue.builder().nul(true).build();
    }
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
      rawMap.forEach((k, v) -> nested.put(k.toString(), wrap(v))); // nulls handled above
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
