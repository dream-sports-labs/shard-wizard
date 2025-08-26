package com.dream11.shardwizard.shardmanager.impl;

import static com.dream11.shardwizard.constant.Constants.Event.*;
import static com.dream11.shardwizard.constant.Constants.Metric.*;

import com.dream11.shardwizard.config.SqlConfig;
import com.dream11.shardwizard.constant.MySqlQueries;
import com.dream11.shardwizard.dto.EntityShardDetailsMapping;
import com.dream11.shardwizard.dto.ObservabilityEvent;
import com.dream11.shardwizard.dto.ShardConfig;
import com.dream11.shardwizard.dto.ShardDetails;
import com.dream11.shardwizard.dto.ShardManagerResponse;
import com.dream11.shardwizard.dto.ShardUpdateResponse;
import com.dream11.shardwizard.exception.DefaultShardNotFoundException;
import com.dream11.shardwizard.exception.EntityNotMappedToShardException;
import com.dream11.shardwizard.metric.ObservabilityServiceFactory;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.reactivex.CompletableHelper;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.mysqlclient.MySQLClient;
import io.vertx.reactivex.mysqlclient.MySQLPool;
import io.vertx.reactivex.sqlclient.*;
import io.vertx.sqlclient.PoolOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardManagerClientImplMySql implements ShardManagerClient {

  private final Vertx vertx;
  private MySQLPool masterPool;
  private MySQLPool slavePool;
  private final MySQLConnectOptions masterOptions;
  private final MySQLConnectOptions slaveOptions;
  private final PoolOptions poolOptions;

  public ShardManagerClientImplMySql(Vertx vertx, SqlConfig sqlConfig) {
    this.vertx = vertx;

    JsonObject config = getMySqlConfigJson(sqlConfig);

    this.masterOptions =
        new MySQLConnectOptions()
            .setHost(config.getString("masterHost"))
            .setPort(config.getInteger("port"))
            .setDatabase(config.getString("database"))
            .setUser(config.getString("username"))
            .setPassword(config.getString("password"))
            .setCachePreparedStatements(config.getBoolean("cachePreparedStatements", false));

    this.slaveOptions =
        new MySQLConnectOptions()
            .setHost(config.getString("slaveHost"))
            .setPort(config.getInteger("port"))
            .setDatabase(config.getString("database"))
            .setUser(config.getString("username"))
            .setPassword(config.getString("password"))
            .setCachePreparedStatements(config.getBoolean("cachePreparedStatements", false));

    this.poolOptions = new PoolOptions().setMaxSize(config.getInteger("maxPoolSize", 1));
  }

  private JsonObject getMySqlConfigJson(SqlConfig sqlConfig) {
    return new JsonObject()
        .put("masterHost", sqlConfig.getWriterHost())
        .put("slaveHost", sqlConfig.getReaderHost())
        .put("database", sqlConfig.getDatabase())
        .put("username", sqlConfig.getUsername())
        .put("password", sqlConfig.getPassword())
        .put("port", sqlConfig.getPort())
        .put("cachePreparedStatements", false)
        .put("maxPoolSize", 1);
  }

  @Override
  public Completable rxBootstrap() {
    return vertx
        .rxExecuteBlocking(
            promise -> {
              try {
                createMasterSlavePool();
                log.info("Successfully Connected to MySQL writer and reader clients");
                promise.complete();
              } catch (Exception e) {
                String text =
                    String.format(
                        "ShardManager:Failed to connect to ShardManager MySQL writer and reader clients, with error: %s",
                        e.getMessage());
                log.error(text);

                ObservabilityEvent observabilityEvent =
                    ObservabilityEvent.builder()
                        .title(DB_CONNECT)
                        .description(text)
                        .severity(ObservabilityEvent.EventSeverity.ERROR)
                        .tags(Map.of("event", DB_CONNECT))
                        .build();
                ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
                promise.fail(e);
              }
            })
        .ignoreElement();
  }

  @Override
  public Completable rxClose() {
    return CompletableHelper.toCompletable(
        handler ->
            this.vertx.executeBlocking(
                promise -> {
                  try {
                    if (masterPool != null) {
                      masterPool.close();
                    }
                    if (slavePool != null) {
                      slavePool.close();
                    }
                    log.info("Client pools have been closed");
                    promise.complete();
                  } catch (Exception e) {
                    String text =
                        "ShardManager:Failed to close writer and reader MySQL ShardManager connections";
                    log.error(text, e);

                    ObservabilityEvent observabilityEvent =
                        ObservabilityEvent.builder()
                            .title(DB_CLOSE)
                            .description(String.format(text + ": %s", e.getMessage()))
                            .severity(ObservabilityEvent.EventSeverity.ERROR)
                            .tags(Map.of("event", DB_CLOSE))
                            .build();
                    ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
                    promise.fail(e);
                  }
                },
                handler));
  }

  @Override
  public Single<ShardDetails> rxRegisterNewShard(boolean isDefault, ShardConfig details) {
    return masterPool
        .rxBegin()
        .flatMap(
            tx ->
                tx.preparedQuery(MySqlQueries.CREATE_SHARD_QUERY)
                    .rxExecute(Tuple.of(isDefault, JsonObject.mapFrom(details).toString()))
                    .map(rs -> rs.property(MySQLClient.LAST_INSERTED_ID))
                    .flatMap(
                        createdShardId ->
                            tx.rxCommit()
                                .toSingleDefault(new ShardDetails(createdShardId, details)))
                    .onErrorResumeNext(
                        err -> {
                          String text =
                              String.format("Failed to insert new shard. %s", err.getMessage());
                          log.error(text);

                          ObservabilityEvent observabilityEvent =
                              ObservabilityEvent.builder()
                                  .title(SHARD_REGISTRATION_FAILED)
                                  .description(text)
                                  .severity(ObservabilityEvent.EventSeverity.ERROR)
                                  .tags(Map.of("event", SHARD_REGISTRATION_FAILED))
                                  .build();
                          ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
                          return tx.rxRollback().andThen(Single.error(err));
                        }));
  }

  @Override
  public Single<ShardManagerResponse> rxSetDefaultFlag(List<Long> shardIds, boolean isDefault) {
    return Single.just(1)
        .flatMap(
            any -> {
              String shardIdsStr =
                  shardIds.stream().map(String::valueOf).collect(Collectors.joining(", "));
              String updateQuery =
                  MySqlQueries.SET_DEFAULT_FLAG_QUERY_PREFIX.replace("?", "" + isDefault)
                      + shardIdsStr
                      + ");";

              return masterPool
                  .query(updateQuery)
                  .rxExecute()
                  .map(rs -> new ShardManagerResponse(true, "Default flag updated."));
            });
  }

  @Override
  public Single<ShardManagerResponse> rxDeactivateShard(long shardId) {
    return masterPool
        .preparedQuery(MySqlQueries.DEACTIVATE_SHARD_QUERY)
        .rxExecute(Tuple.of(shardId))
        .map(
            rows -> {
              if (rows.rowCount() == 0) {
                return new ShardManagerResponse(true, "Shard is already linked to an entity");
              }
              return new ShardManagerResponse(true, "Shard deactivated");
            });
  }

  @Override
  public Single<List<ShardDetails>> rxGetActiveShards() {
    return slavePool
        .preparedQuery(MySqlQueries.GET_ALL_SHARDS_QUERY)
        .rxExecute(Tuple.of(true))
        .map(ShardManagerClientImplMySql::getShardDetailsFromRowSet);
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMappingOrCreateDefault(String entityId) {
    return masterPool
        .rxBegin()
        .flatMap(
            tx ->
                tx.preparedQuery(MySqlQueries.GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY)
                    .rxExecute(Tuple.of(entityId))
                    .map(ShardManagerClientImplMySql::getShardDetailsFromRowSet)
                    .flatMap(
                        shardDetails -> {
                          if (shardDetails.isEmpty()) {
                            return createDefaultEntityShardMapping(tx, entityId);
                          }
                          return Single.just(new EntityShardDetailsMapping(entityId, shardDetails));
                        })
                    .flatMap(x -> tx.rxCommit().toSingleDefault(x))
                    .onErrorResumeNext(
                        err -> {
                          String text =
                              String.format(
                                  "Failed to insert new shard for entityId: %s and error message: %s",
                                  entityId, err.getMessage());
                          log.error(text);

                          ObservabilityEvent observabilityEvent =
                              ObservabilityEvent.builder()
                                  .title(SHARD_INSERTION_FAILED)
                                  .description(text)
                                  .severity(ObservabilityEvent.EventSeverity.ERROR)
                                  .tags(
                                      Map.of("entityId", entityId, "event", SHARD_INSERTION_FAILED))
                                  .build();
                          ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
                          return tx.rxRollback().andThen(Single.error(err));
                        }));
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMapping(String entityId) {
    return masterPool
        .preparedQuery(MySqlQueries.GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY)
        .rxExecute(Tuple.of(entityId))
        .map(ShardManagerClientImplMySql::getShardDetailsFromRowSet)
        .flatMap(
            shardDetails -> {
              if (shardDetails.isEmpty()) {
                throw new EntityNotMappedToShardException(entityId);
              } else {
                shardDetails.sort(Comparator.comparingLong(ShardDetails::getShardId));
                EntityShardDetailsMapping entityShardDetailsMapping =
                    new EntityShardDetailsMapping(entityId, shardDetails);
                log.debug(
                    "ShardManager:Found EntityShardMapping for entity {}",
                    entityId,
                    "with details:{}",
                    shardDetails);
                return Single.just(entityShardDetailsMapping);
              }
            });
  }

  private Single<EntityShardDetailsMapping> createDefaultEntityShardMapping(
      Transaction tx, String entityId) {
    return tx.preparedQuery(MySqlQueries.GET_DEFAULT_SHARDS_QUERY)
        .rxExecute()
        .map(ShardManagerClientImplMySql::getShardDetailsFromRowSet)
        .flatMap(
            defaultShards -> {
              if (defaultShards.isEmpty()) {
                return Single.error(new DefaultShardNotFoundException());
              }
              List<Long> defaultShardIds =
                  defaultShards.stream().map(ShardDetails::getShardId).collect(Collectors.toList());
              return tx.preparedQuery(MySqlQueries.CREATE_ENTITY_SHARD_MAPPING_QUERY)
                  .rxExecute(Tuple.of(entityId, new JsonArray(defaultShardIds).encode()))
                  .map(res -> new EntityShardDetailsMapping(entityId, defaultShards));
            });
  }

  @Override
  public Single<List<EntityShardDetailsMapping>> rxListEntityShardMappings() {
    return slavePool
        .preparedQuery(MySqlQueries.GET_SHARD_DETAILS_FOR_ALL_ENTITIES_QUERY)
        .rxExecute()
        .map(ShardManagerClientImplMySql::getEntityIdToActiveShardsMap)
        .map(
            entityIdToShardDetailsMap ->
                entityIdToShardDetailsMap.entrySet().stream()
                    .map(entry -> new EntityShardDetailsMapping(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()));
  }

  @Override
  public Single<ShardManagerResponse> rxDeleteEntityShardMapping(String entityId) {
    return masterPool
        .preparedQuery(MySqlQueries.DELETE_ENTITY_SHARD_MAPPING_QUERY)
        .rxExecute(Tuple.of(entityId))
        .map(
            rows -> {
              if (rows.rowCount() == 0) {
                String text =
                    String.format("EntityShardMapping not found for entityId: %s", entityId);
                ObservabilityEvent observabilityEvent =
                    ObservabilityEvent.builder()
                        .title(ENTITY_SHARD_MAPPING_NOT_FOUND)
                        .description(text)
                        .severity(ObservabilityEvent.EventSeverity.ERROR)
                        .tags(Map.of("entityId", entityId, "event", ENTITY_SHARD_MAPPING_NOT_FOUND))
                        .build();
                ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
                throw new IllegalArgumentException("EntityShardMapping not found.");
              }
              return new ShardManagerResponse(true, "EntityShardMapping deleted");
            });
  }

  @Override
  public Single<ShardManagerResponse> rxEstablishEntityToShardsMapping(
      String entityId, List<Long> shardIds) {
    return rxGetActiveShards()
        .flatMap(
            activeShardIds -> {
              Set<Long> activeShardIdsSet =
                  activeShardIds.stream().map(ShardDetails::getShardId).collect(Collectors.toSet());
              if (!activeShardIdsSet.containsAll(shardIds)) {
                return Single.error(new IllegalArgumentException("Invalid shardIds."));
              }
              return masterPool
                  .preparedQuery(MySqlQueries.CREATE_ENTITY_SHARD_MAPPING_QUERY)
                  .rxExecute(Tuple.of(entityId, new JsonArray(shardIds).encode()));
            })
        .map(any -> new ShardManagerResponse(true, "Mapping created"));
  }

  @Override
  public Single<ShardUpdateResponse> rxUpdateExistingShardDetails(
      long shardId, ShardConfig details) {
    return masterPool
        .rxBegin()
        .flatMap(
            tx -> {
              // First get current details
              return tx.preparedQuery(MySqlQueries.GET_SHARD_DETAILS_QUERY)
                  .rxExecute(Tuple.of(shardId))
                  .flatMap(
                      currentResult -> {
                        List<ShardDetails> currentShards = getShardDetailsFromRowSet(currentResult);
                        if (currentShards.isEmpty()) {
                          return Single.error(
                              new IllegalArgumentException("Shard not found with id: " + shardId));
                        }
                        ShardDetails currentShard = currentShards.get(0);
                        return tx.preparedQuery(MySqlQueries.SET_SHARD_DETAILS_QUERY)
                            .rxExecute(Tuple.of(JsonObject.mapFrom(details).toString(), shardId))
                            .flatMap(
                                updateResult -> {
                                  if (updateResult.rowCount() == 0) {
                                    return Single.error(
                                        new IllegalArgumentException(
                                            "Failed to update shard with id: " + shardId));
                                  }
                                  // Then fetch the updated details
                                  return tx.preparedQuery(MySqlQueries.GET_SHARD_DETAILS_QUERY)
                                      .rxExecute(Tuple.of(shardId))
                                      .map(
                                          rows -> {
                                            List<ShardDetails> updatedShards =
                                                getShardDetailsFromRowSet(rows);
                                            if (updatedShards.isEmpty()) {
                                              throw new IllegalArgumentException(
                                                  "Failed to parse updated shard details");
                                            }
                                            ShardDetails updatedShard = updatedShards.get(0);
                                            log.info("Updated shard details: {}", updatedShard);
                                            return ShardUpdateResponse.builder()
                                                .currentShardDetails(currentShard)
                                                .updatedShardDetails(updatedShard)
                                                .build();
                                          });
                                });
                      })
                  .flatMap(updateResponse -> tx.rxCommit().toSingleDefault(updateResponse))
                  .onErrorResumeNext(
                      err -> {
                        log.error("ShardManager:Failed to update shard details", err);
                        return tx.rxRollback().andThen(Single.error(err));
                      });
            });
  }

  private static List<ShardDetails> getShardDetailsFromRowSet(RowSet<Row> rows) {
    List<ShardDetails> shardDetails = new ArrayList<>();
    for (Row row : rows) {
      long shardId = row.getLong("ShardId");

      JsonObject shardMetaJson = (JsonObject) row.getValue("Details");
      ShardConfig shardConfig = shardMetaJson.mapTo(ShardConfig.class);
      shardDetails.add(new ShardDetails(shardId, shardConfig));
    }
    return shardDetails;
  }

  private static Map<String, List<ShardDetails>> getEntityIdToActiveShardsMap(RowSet<Row> rows) {
    Map<String, List<ShardDetails>> entityIdToShardDetailsMap = new HashMap<>();
    for (Row row : rows) {
      String entityId = row.getString("EntityId");
      long shardId = row.getLong("ShardId");
      JsonObject shardMetaJson = (JsonObject) row.getValue("Details");
      ShardConfig shardConfig = shardMetaJson.mapTo(ShardConfig.class);
      ShardDetails shardDetails = new ShardDetails(shardId, shardConfig);
      entityIdToShardDetailsMap.computeIfAbsent(entityId, k -> new ArrayList<>()).add(shardDetails);
    }
    return entityIdToShardDetailsMap;
  }

  private void createMasterSlavePool() {
    try {
      masterPool = MySQLPool.pool(vertx, masterOptions, poolOptions);
      slavePool = MySQLPool.pool(vertx, slaveOptions, poolOptions);
      log.info("POOL CREATED: master --> {}  slave --> {}", masterPool, slavePool);
    } catch (Exception e) {
      log.error("Error in createMasterSlavePool {}", e.getMessage());
    }
  }
}
