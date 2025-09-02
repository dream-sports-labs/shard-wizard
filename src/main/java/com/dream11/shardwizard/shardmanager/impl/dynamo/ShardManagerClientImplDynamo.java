package com.dream11.shardwizard.shardmanager.impl.dynamo;

import com.dream11.shardwizard.config.DynamoConfig;
import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.exception.DefaultShardNotFoundException;
import com.dream11.shardwizard.exception.EntityNotMappedToShardException;
import com.dream11.shardwizard.model.EntityShardDetailsMapping;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import com.dream11.shardwizard.model.ShardManagerResponse;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

@Slf4j
public class ShardManagerClientImplDynamo implements ShardManagerClient {

  private final DynamoDbAsyncClient dynamoClient;

  public static final String SHARD_MASTER_TABLE = "ShardMaster";
  public static final String ENTITY_SHARD_MAPPING_TABLE = "EntityShardMapping";

  // Shard Master table columns
  public static final String SHARD_ID = "shard_id";
  public static final String IS_ACTIVE = "is_active";
  public static final String IS_DEFAULT = "is_default";
  public static final String DETAILS = "details";
  public static final String CREATED_AT = "created_at";
  public static final String UPDATED_AT = "updated_at";

  // Entity Shard Mapping table columns
  public static final String ENTITY_ID = "entity_id";
  public static final String SHARD_IDS = "shard_ids";

  // Shard connection parameter keys
  public static final String WRITER_HOST = "writerHost";
  public static final String READER_HOST = "readerHost";
  public static final String PORT = "port";
  public static final String MAX_CONNECTIONS = "maxConnections";
  public static final String MAX_WAIT_QUEUE_SIZE = "maxWaitQueueSize";
  public static final String CONNECTION_TIMEOUT_MS = "connectionTimeoutMs";
  public static final String USERNAME = "username";
  public static final String PASSWORD = "password";
  public static final String DATABASE = "database";
  public static final String ACCESS_KEY = "accessKey";
  public static final String SECRET_KEY = "secretKey";
  public static final String DATABASE_TYPE = "databaseType";
  public static final String ENDPOINT = "endpoint";
  public static final String REGION = "region";
  public static final String SHARD_CONNECTION_PARAMS = "shardConnectionParams";

  public ShardManagerClientImplDynamo(DynamoConfig dynamoConfig) {

    DynamoDbAsyncClientBuilder builder =
        DynamoDbAsyncClient.builder()
            .httpClientBuilder(AwsCrtAsyncHttpClient.builder())
            .region(Region.of(dynamoConfig.getRegion()));

    configureCredentials(dynamoConfig, builder);

    configureEndpoint(dynamoConfig, builder);

    this.dynamoClient = builder.build();
  }

  private static void configureEndpoint(
      DynamoConfig dynamoConfig, DynamoDbAsyncClientBuilder builder) {
    if (dynamoConfig.getEndpointOverride() != null
        && !dynamoConfig.getEndpointOverride().trim().isEmpty()) {
      builder.endpointOverride(URI.create(dynamoConfig.getEndpointOverride()));
    }
  }

  private static void configureCredentials(
      DynamoConfig dynamoConfig, DynamoDbAsyncClientBuilder builder) {
    if (dynamoConfig.getAccessKey() != null
        && !dynamoConfig.getAccessKey().isBlank()
        && dynamoConfig.getSecretKey() != null
        && !dynamoConfig.getSecretKey().isBlank()) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(
              AwsBasicCredentials.create(
                  dynamoConfig.getAccessKey(), dynamoConfig.getSecretKey())));
    }
  }

  @Override
  public Completable rxBootstrap() {
    return Completable.complete();
  }

  @Override
  public Completable rxClose() {
    return Completable.complete();
  }

  @Override
  public Single<ShardDetails> rxRegisterNewShard(boolean isDefault, ShardConfig details) {
    long shardId = System.currentTimeMillis();

    ShardConnectionParameters params = details.getShardConnectionParams();
    Map<String, AttributeValue> shardConnectionMap = new HashMap<>();

    shardConnectionMap.put(WRITER_HOST, AttributeValue.builder().s(params.getWriterHost()).build());
    shardConnectionMap.put(READER_HOST, AttributeValue.builder().s(params.getReaderHost()).build());
    shardConnectionMap.put(
        PORT, AttributeValue.builder().n(String.valueOf(params.getPort())).build());
    shardConnectionMap.put(
        MAX_CONNECTIONS,
        AttributeValue.builder().n(String.valueOf(params.getMaxConnections())).build());
    shardConnectionMap.put(
        MAX_WAIT_QUEUE_SIZE,
        AttributeValue.builder().n(String.valueOf(params.getMaxWaitQueueSize())).build());
    shardConnectionMap.put(
        CONNECTION_TIMEOUT_MS,
        AttributeValue.builder().n(String.valueOf(params.getConnectionTimeoutMs())).build());
    shardConnectionMap.put(USERNAME, AttributeValue.builder().s(params.getUsername()).build());
    shardConnectionMap.put(PASSWORD, AttributeValue.builder().s(params.getPassword()).build());
    shardConnectionMap.put(DATABASE, AttributeValue.builder().s(params.getDatabase()).build());
    shardConnectionMap.put(ACCESS_KEY, AttributeValue.builder().s(params.getAccessKey()).build());
    shardConnectionMap.put(SECRET_KEY, AttributeValue.builder().s(params.getSecretKey()).build());

    Map<String, AttributeValue> detailsMap =
        Map.of(
            DATABASE_TYPE, AttributeValue.builder().s(details.getDatabaseType().toString()).build(),
            SHARD_CONNECTION_PARAMS, AttributeValue.builder().m(shardConnectionMap).build());

    Map<String, AttributeValue> item =
        Map.of(
            SHARD_ID, AttributeValue.builder().n(String.valueOf(shardId)).build(),
            IS_ACTIVE, AttributeValue.builder().bool(true).build(),
            IS_DEFAULT, AttributeValue.builder().bool(isDefault).build(),
            DETAILS, AttributeValue.builder().m(detailsMap).build(),
            CREATED_AT, AttributeValue.builder().s(Instant.now().toString()).build(),
            UPDATED_AT, AttributeValue.builder().s(Instant.now().toString()).build());

    PutItemRequest request =
        PutItemRequest.builder().tableName(SHARD_MASTER_TABLE).item(item).build();

    return Single.fromFuture(dynamoClient.putItem(request))
        .map(response -> new ShardDetails(shardId, details));
  }

  @Override
  public Single<ShardManagerResponse> rxSetDefaultFlag(List<Long> shardIds, boolean isDefault) {
    List<Single<Boolean>> updates =
        shardIds.stream()
            .map(
                shardId -> {
                  Map<String, AttributeValue> key =
                      Map.of(SHARD_ID, AttributeValue.builder().n(String.valueOf(shardId)).build());
                  Map<String, AttributeValueUpdate> updatesMap =
                      Map.of(
                          IS_DEFAULT,
                          AttributeValueUpdate.builder()
                              .value(AttributeValue.builder().bool(isDefault).build())
                              .action(AttributeAction.PUT)
                              .build());
                  return dynamoClient
                      .updateItem(
                          UpdateItemRequest.builder()
                              .tableName(SHARD_MASTER_TABLE)
                              .key(key)
                              .attributeUpdates(updatesMap)
                              .build())
                      .thenApply(resp -> true);
                })
            .map(Single::fromFuture)
            .collect(Collectors.toList());

    return Single.zip(updates, ignored -> new ShardManagerResponse(true, "Default flags updated."));
  }

  @Override
  public Single<ShardManagerResponse> rxDeactivateShard(long shardId) {
    Map<String, AttributeValue> key =
        Map.of(SHARD_ID, AttributeValue.builder().n(String.valueOf(shardId)).build());
    Map<String, AttributeValueUpdate> updates =
        Map.of(
            IS_ACTIVE,
            AttributeValueUpdate.builder()
                .value(AttributeValue.builder().bool(false).build())
                .action(AttributeAction.PUT)
                .build());
    return Single.fromFuture(
            dynamoClient.updateItem(
                UpdateItemRequest.builder()
                    .tableName(SHARD_MASTER_TABLE)
                    .key(key)
                    .attributeUpdates(updates)
                    .build()))
        .map(resp -> new ShardManagerResponse(true, "Shard deactivated"));
  }

  @Override
  public Single<List<ShardDetails>> rxGetActiveShards() {
    ScanRequest scan =
        ScanRequest.builder()
            .tableName(SHARD_MASTER_TABLE)
            .filterExpression("is_active = :active")
            .expressionAttributeValues(
                Map.of(":active", AttributeValue.builder().bool(true).build()))
            .build();

    return Single.fromFuture(dynamoClient.scan(scan))
        .map(
            resp ->
                resp.items().stream()
                    .map(
                        item -> {
                          long shardId = Long.parseLong(item.get(SHARD_ID).n());
                          Map<String, AttributeValue> detailsAttr = item.get(DETAILS).m();
                          // Deserialize `detailsAttr` to `ShardConfig` manually
                          ShardConfig config =
                              parseShardConfig(detailsAttr); // You’d implement this
                          return new ShardDetails(shardId, config);
                        })
                    .collect(Collectors.toList()));
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMappingOrCreateDefault(String entityId) {
    return rxFindMapping(entityId)
        .onErrorResumeNext(
            error -> {
              if (!(error instanceof EntityNotMappedToShardException)) {
                return Single.error(error);
              }

              return rxGetDefaultShards()
                  .flatMap(
                      defaultShards -> {
                        if (defaultShards.isEmpty()) {
                          return Single.error(new DefaultShardNotFoundException());
                        }

                        List<Long> shardIds =
                            defaultShards.stream()
                                .map(ShardDetails::getShardId)
                                .collect(Collectors.toList());

                        return rxCreateEntityShardMapping(entityId, shardIds)
                            .toSingleDefault(
                                new EntityShardDetailsMapping(entityId, defaultShards));
                      });
            });
  }

  private Completable rxCreateEntityShardMapping(String entityId, List<Long> shardIds) {
    List<AttributeValue> shardIdList =
        shardIds.stream()
            .map(id -> AttributeValue.builder().n(String.valueOf(id)).build())
            .collect(Collectors.toList());

    Map<String, AttributeValue> item =
        Map.of(
            ENTITY_ID, AttributeValue.builder().s(entityId).build(),
            SHARD_IDS, AttributeValue.builder().l(shardIdList).build(),
            CREATED_AT, AttributeValue.builder().s(Instant.now().toString()).build(),
            UPDATED_AT, AttributeValue.builder().s(Instant.now().toString()).build());

    return Completable.fromFuture(
        dynamoClient.putItem(
            PutItemRequest.builder().tableName(ENTITY_SHARD_MAPPING_TABLE).item(item).build()));
  }

  private Single<List<ShardDetails>> rxGetDefaultShards() {
    ScanRequest request =
        ScanRequest.builder()
            .tableName(SHARD_MASTER_TABLE)
            .filterExpression("is_default = :d AND is_active = :a")
            .expressionAttributeValues(
                Map.of(
                    ":d", AttributeValue.builder().bool(true).build(),
                    ":a", AttributeValue.builder().bool(true).build()))
            .build();

    return Single.fromFuture(dynamoClient.scan(request))
        .map(
            response ->
                response.items().stream()
                    .map(
                        item -> {
                          long shardId = Long.parseLong(item.get(SHARD_ID).n());
                          Map<String, AttributeValue> detailsMap = item.get(DETAILS).m();
                          ShardConfig config =
                              parseShardConfig(detailsMap); // You should already have this
                          return new ShardDetails(shardId, config);
                        })
                    .collect(Collectors.toList()));
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMapping(String entityId) {
    Map<String, AttributeValue> key =
        Map.of(ENTITY_ID, AttributeValue.builder().s(entityId).build());

    return Single.fromFuture(
            dynamoClient.getItem(
                GetItemRequest.builder().tableName(ENTITY_SHARD_MAPPING_TABLE).key(key).build()))
        .flatMap(
            resp -> {
              if (resp.item() == null || resp.item().isEmpty()) {
                return Single.error(new EntityNotMappedToShardException(entityId));
              }
              List<Long> shardIds =
                  resp.item().get(SHARD_IDS).l().stream()
                      .map(attr -> Long.parseLong(attr.n()))
                      .collect(Collectors.toList());
              return getShardDetailsByIds(shardIds)
                  .map(details -> new EntityShardDetailsMapping(entityId, details));
            });
  }

  @Override
  public Single<List<EntityShardDetailsMapping>> rxListEntityShardMappings() {
    ScanRequest scan = ScanRequest.builder().tableName(ENTITY_SHARD_MAPPING_TABLE).build();

    return Single.fromFuture(dynamoClient.scan(scan))
        .flatMap(
            resp -> {
              List<Single<EntityShardDetailsMapping>> mappings = new ArrayList<>();
              for (Map<String, AttributeValue> item : resp.items()) {
                String entityId = item.get(ENTITY_ID).s();
                List<Long> shardIds =
                    item.get(SHARD_IDS).l().stream()
                        .map(v -> Long.parseLong(v.n()))
                        .collect(Collectors.toList());
                mappings.add(
                    getShardDetailsByIds(shardIds)
                        .map(details -> new EntityShardDetailsMapping(entityId, details)));
              }
              return Single.zip(
                  mappings,
                  list ->
                      Arrays.stream(list)
                          .map(e -> (EntityShardDetailsMapping) e)
                          .collect(Collectors.toList()));
            });
  }

  @Override
  public Single<ShardManagerResponse> rxDeleteEntityShardMapping(String entityId) {
    return Single.fromFuture(
            dynamoClient.deleteItem(
                DeleteItemRequest.builder()
                    .tableName(ENTITY_SHARD_MAPPING_TABLE)
                    .key(Map.of(ENTITY_ID, AttributeValue.builder().s(entityId).build()))
                    .build()))
        .map(resp -> new ShardManagerResponse(true, "EntityShardMapping deleted"));
  }

  @Override
  public Single<ShardManagerResponse> rxEstablishEntityToShardsMapping(
      String entityId, List<Long> shardIds) {
    List<AttributeValue> shardIdList =
        shardIds.stream()
            .map(id -> AttributeValue.builder().n(String.valueOf(id)).build())
            .collect(Collectors.toList());

    Map<String, AttributeValue> item =
        Map.of(
            ENTITY_ID, AttributeValue.builder().s(entityId).build(),
            SHARD_IDS, AttributeValue.builder().l(shardIdList).build(),
            CREATED_AT, AttributeValue.builder().s(Instant.now().toString()).build(),
            UPDATED_AT, AttributeValue.builder().s(Instant.now().toString()).build());

    return Single.fromFuture(
            dynamoClient.putItem(
                PutItemRequest.builder().tableName(ENTITY_SHARD_MAPPING_TABLE).item(item).build()))
        .map(resp -> new ShardManagerResponse(true, "Mapping created"));
  }

  private Single<List<ShardDetails>> getShardDetailsByIds(List<Long> shardIds) {
    List<Single<ShardDetails>> shardFetches =
        shardIds.stream()
            .map(
                id -> {
                  return Single.fromFuture(
                          dynamoClient.getItem(
                              GetItemRequest.builder()
                                  .tableName(SHARD_MASTER_TABLE)
                                  .key(
                                      Map.of(
                                          SHARD_ID,
                                          AttributeValue.builder().n(String.valueOf(id)).build()))
                                  .build()))
                      .map(
                          resp -> {
                            Map<String, AttributeValue> item = resp.item();
                            Map<String, AttributeValue> detailsAttr = item.get(DETAILS).m();
                            ShardConfig config = parseShardConfig(detailsAttr); // You implement
                            return new ShardDetails(id, config);
                          });
                })
            .collect(Collectors.toList());

    return Single.zip(
        shardFetches,
        list -> Arrays.stream(list).map(v -> (ShardDetails) v).collect(Collectors.toList()));
  }

  private ShardConfig parseShardConfig(Map<String, AttributeValue> detailsMap) {
    String databaseTypeStr = detailsMap.get(DATABASE_TYPE).s();
    Map<String, AttributeValue> shardConnMap = detailsMap.get(SHARD_CONNECTION_PARAMS).m();

    ShardConnectionParameters.ShardConnectionParametersBuilder connBuilder =
        ShardConnectionParameters.builder()
            .writerHost(shardConnMap.get(WRITER_HOST).s())
            .readerHost(shardConnMap.get(READER_HOST).s())
            .username(
                shardConnMap.getOrDefault(USERNAME, AttributeValue.builder().s(null).build()).s())
            .password(
                shardConnMap.getOrDefault(PASSWORD, AttributeValue.builder().s(null).build()).s())
            .database(
                shardConnMap.getOrDefault(DATABASE, AttributeValue.builder().s(null).build()).s())
            .port(Integer.parseInt(shardConnMap.get(PORT).n()))
            .maxConnections(Integer.parseInt(shardConnMap.get(MAX_CONNECTIONS).n()))
            .maxWaitQueueSize(Integer.parseInt(shardConnMap.get(MAX_WAIT_QUEUE_SIZE).n()))
            .connectionTimeoutMs(Integer.parseInt(shardConnMap.get(CONNECTION_TIMEOUT_MS).n()))
            .accessKey(
                shardConnMap.getOrDefault(ACCESS_KEY, AttributeValue.builder().s("").build()).s())
            .secretKey(
                shardConnMap.getOrDefault(SECRET_KEY, AttributeValue.builder().s("").build()).s())
            .endpoint(
                shardConnMap.getOrDefault(ENDPOINT, AttributeValue.builder().s(null).build()).s())
            .region(
                shardConnMap.getOrDefault(REGION, AttributeValue.builder().s(null).build()).s());

    return ShardConfig.builder()
        .databaseType(DatabaseType.valueOf(databaseTypeStr))
        .shardConnectionParams(connBuilder.build())
        .build();
  }

  @Override
  public Single<ShardUpdateResponse> rxUpdateExistingShardDetails(
      long shardId, ShardConfig details) {
    log.info("Starting update of shard details for shardId: {} with details: {}", shardId, details);

    // First get current details
    Map<String, AttributeValue> key = new HashMap<>();
    key.put("shard_id", AttributeValue.builder().n(String.valueOf(shardId)).build());

    GetItemRequest getRequest =
        GetItemRequest.builder().tableName(SHARD_MASTER_TABLE).key(key).build();

    return Single.fromFuture(dynamoClient.getItem(getRequest))
        .flatMap(
            getResponse -> {
              if (!getResponse.hasItem()) {
                log.error("Shard not found with id: {}", shardId);
                return Single.error(
                    new IllegalArgumentException("Shard not found with id: " + shardId));
              }

              // Parse current shard details
              Map<String, AttributeValue> currentItem = getResponse.item();
              Map<String, AttributeValue> currentDetailsMap = currentItem.get("details").m();
              ShardConfig currentConfig = parseShardConfig(currentDetailsMap);
              ShardDetails currentShard = new ShardDetails(shardId, currentConfig);

              // Prepare update request
              Map<String, AttributeValue> detailsMap = new HashMap<>();
              detailsMap.put(
                  "databaseType",
                  AttributeValue.builder().s(details.getDatabaseType().name()).build());
              detailsMap.put(
                  "shardConnectionParams",
                  AttributeValue.builder()
                      .m(createShardConnectionMap(details.getShardConnectionParams()))
                      .build());

              Map<String, String> expressionAttributeNames = new HashMap<>();
              expressionAttributeNames.put("#details", "details");
              expressionAttributeNames.put("#updatedAt", "updated_at");
              expressionAttributeNames.put("#shardId", "shard_id");

              Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
              expressionAttributeValues.put(
                  ":details", AttributeValue.builder().m(detailsMap).build());
              expressionAttributeValues.put(
                  ":updatedAt", AttributeValue.builder().s(Instant.now().toString()).build());

              UpdateItemRequest updateRequest =
                  UpdateItemRequest.builder()
                      .tableName(SHARD_MASTER_TABLE)
                      .key(key)
                      .updateExpression("SET #details = :details, #updatedAt = :updatedAt")
                      .expressionAttributeNames(expressionAttributeNames)
                      .expressionAttributeValues(expressionAttributeValues)
                      .conditionExpression("attribute_exists(#shardId)")
                      .build();

              return Single.fromFuture(dynamoClient.updateItem(updateRequest))
                  .flatMap(
                      updateResponse -> {
                        return Single.fromFuture(dynamoClient.getItem(getRequest))
                            .map(
                                updatedResponse -> {
                                  Map<String, AttributeValue> updatedItem = updatedResponse.item();
                                  Map<String, AttributeValue> updatedDetailsMap =
                                      updatedItem.get("details").m();
                                  ShardConfig updatedConfig = parseShardConfig(updatedDetailsMap);
                                  ShardDetails updatedShard =
                                      new ShardDetails(shardId, updatedConfig);

                                  return ShardUpdateResponse.builder()
                                      .currentShardDetails(currentShard)
                                      .updatedShardDetails(updatedShard)
                                      .build();
                                });
                      })
                  .onErrorResumeNext(
                      error -> {
                        if (error instanceof ConditionalCheckFailedException) {
                          log.error(
                              "Conditional check failed for shard update with id: {}", shardId);
                          return Single.error(
                              new IllegalArgumentException(
                                  "Failed to update shard with id: " + shardId));
                        }
                        log.error("Failed to update shard details for shardId: {}", shardId, error);
                        return Single.error(
                            new RuntimeException("Failed to update shard details", error));
                      });
            });
  }

  private Map<String, AttributeValue> createShardConnectionMap(ShardConnectionParameters params) {
    Map<String, AttributeValue> shardConnectionMap = new HashMap<>();
    shardConnectionMap.put(WRITER_HOST, AttributeValue.builder().s(params.getWriterHost()).build());
    shardConnectionMap.put(READER_HOST, AttributeValue.builder().s(params.getReaderHost()).build());
    shardConnectionMap.put(
        PORT, AttributeValue.builder().n(String.valueOf(params.getPort())).build());
    shardConnectionMap.put(
        MAX_CONNECTIONS,
        AttributeValue.builder().n(String.valueOf(params.getMaxConnections())).build());
    shardConnectionMap.put(
        MAX_WAIT_QUEUE_SIZE,
        AttributeValue.builder().n(String.valueOf(params.getMaxWaitQueueSize())).build());
    shardConnectionMap.put(
        CONNECTION_TIMEOUT_MS,
        AttributeValue.builder().n(String.valueOf(params.getConnectionTimeoutMs())).build());
    shardConnectionMap.put(USERNAME, AttributeValue.builder().s(params.getUsername()).build());
    shardConnectionMap.put(PASSWORD, AttributeValue.builder().s(params.getPassword()).build());
    shardConnectionMap.put(DATABASE, AttributeValue.builder().s(params.getDatabase()).build());

    if (params.getAccessKey() != null) {
      shardConnectionMap.put(ACCESS_KEY, AttributeValue.builder().s(params.getAccessKey()).build());
    }
    if (params.getSecretKey() != null) {
      shardConnectionMap.put(SECRET_KEY, AttributeValue.builder().s(params.getSecretKey()).build());
    }

    return shardConnectionMap;
  }
}
