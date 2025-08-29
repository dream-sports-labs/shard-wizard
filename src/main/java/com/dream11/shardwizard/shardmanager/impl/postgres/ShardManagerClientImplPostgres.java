package com.dream11.shardwizard.shardmanager.impl.postgres;

import static com.dream11.shardwizard.constant.Constants.CHECK_READONLY_MODE_INTERVAL_SECONDS;
import static com.dream11.shardwizard.constant.Constants.Event.*;
import static com.dream11.shardwizard.constant.Constants.Metric.*;
import static com.dream11.shardwizard.shardmanager.impl.postgres.PostgresQueries.SHOW_TRANSACTION_READ_ONLY;

import com.dream11.shardwizard.config.SqlConfig;
import com.dream11.shardwizard.exception.DefaultShardNotFoundException;
import com.dream11.shardwizard.exception.EntityNotMappedToShardException;
import com.dream11.shardwizard.metric.ObservabilityServiceFactory;
import com.dream11.shardwizard.model.EntityShardDetailsMapping;
import com.dream11.shardwizard.model.ObservabilityEvent;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardDetails;
import com.dream11.shardwizard.model.ShardManagerResponse;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.Transaction;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ShardManagerClientImplPostgres implements ShardManagerClient {

  private final Vertx vertx;
  private PgPool writerPgClient;
  private PgPool readerPgClient;
  private final PgConnectOptions writerConnectOptions;
  private final PgConnectOptions readerConnectOptions;
  private final PoolOptions poolOptions;
  static ObjectMapper objectMapper = new ObjectMapper();
  private long timerId;

  public ShardManagerClientImplPostgres(Vertx vertx, SqlConfig sqlConfig) {
    this.vertx = vertx;

    this.writerConnectOptions =
        new PgConnectOptions()
            .setPort(sqlConfig.getPort())
            .setHost(sqlConfig.getWriterHost())
            .setDatabase(sqlConfig.getDatabase())
            .setUser(sqlConfig.getUsername())
            .setPassword(sqlConfig.getPassword());

    this.readerConnectOptions =
        new PgConnectOptions()
            .setPort(sqlConfig.getPort())
            .setHost(sqlConfig.getReaderHost())
            .setDatabase(sqlConfig.getDatabase())
            .setUser(sqlConfig.getUsername())
            .setPassword(sqlConfig.getPassword());

    this.poolOptions = new PoolOptions().setMaxSize(1);
  }

  @Override
  public Completable rxBootstrap() {
    return Completable.fromFuture(
        CompletableFuture.runAsync(
            () -> {
              try {
                createMasterSlaveConnection();
                long interval =
                    CHECK_READONLY_MODE_INTERVAL_SECONDS * 1000L; // convert to milliseconds
                timerId =
                    vertx.setPeriodic(
                        interval,
                        id -> {
                          try {
                            checkIfMasterInReadOnlyMode();
                          } catch (Exception e) {
                            log.error("Error in periodic readonly check", e);
                          }
                        });
                // Run first check immediately
                vertx.runOnContext(v -> checkIfMasterInReadOnlyMode());
              } catch (Exception e) {
                String text =
                    String.format(
                        "ShardManager:Failed to connect to ShardManager Postgres writer and reader clients, with error: %s",
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
              }
            }));
  }

  private void createMasterSlaveConnection() {
    this.writerPgClient = PgPool.pool(vertx, writerConnectOptions, poolOptions);
    this.readerPgClient = PgPool.pool(vertx, readerConnectOptions, poolOptions);
    log.info("Successfully Connected to ShardManager Postgres writer and reader clients");
  }

  @Override
  public Completable rxClose() {
    return Completable.fromFuture(
        CompletableFuture.runAsync(
            () -> {
              try {
                if (timerId != 0) {
                  vertx.cancelTimer(timerId);
                  timerId = 0;
                }
                Optional.ofNullable(this.writerPgClient).ifPresent(PgPool::close);
                Optional.ofNullable(this.readerPgClient).ifPresent(PgPool::close);
                log.info("Closed writer and reader Postgres ShardManager connections");
              } catch (Exception e) {
                String text =
                    "ShardManager:Failed to close writer and reader Postgres ShardManager connections";
                log.error(text, e);
                ObservabilityEvent observabilityEvent =
                    ObservabilityEvent.builder()
                        .title(DB_CLOSE)
                        .description(String.format(text + ": %s", e.getMessage()))
                        .severity(ObservabilityEvent.EventSeverity.ERROR)
                        .tags(Map.of("event", DB_CLOSE))
                        .build();
                ObservabilityServiceFactory.getInstance().recordEvent(observabilityEvent);
              }
            }));
  }

  @Override
  public Single<ShardDetails> rxRegisterNewShard(boolean isDefault, ShardConfig details) {
    return writerPgClient
        .rxBegin()
        .flatMap(
            tx ->
                tx.preparedQuery(PostgresQueries.CREATE_SHARD_QUERY)
                    .rxExecute(Tuple.of(isDefault, JsonObject.mapFrom(details).toString()))
                    .map(rs -> rs.iterator().next().getLong("shardid"))
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
                  PostgresQueries.SET_DEFAULT_FLAG_QUERY_PREFIX.replace("$1", "" + isDefault)
                      + shardIdsStr
                      + ");";

              return writerPgClient
                  .query(updateQuery)
                  .rxExecute()
                  .map(rs -> new ShardManagerResponse(true, "Default flag updated."));
            });
  }

  @Override
  public Single<ShardManagerResponse> rxDeactivateShard(long shardId) {
    return writerPgClient
        .preparedQuery(PostgresQueries.DEACTIVATE_SHARD_QUERY)
        .rxExecute(Tuple.of(shardId))
        .map(
            rows -> {
              if (rows.rowCount() == 0) {
                return new ShardManagerResponse(
                    true, "ShardManager:Shard is already linked to an entity");
              }
              return new ShardManagerResponse(true, "Shard deactivated");
            });
  }

  @Override
  public Single<List<ShardDetails>> rxGetActiveShards() {
    return readerPgClient
        .preparedQuery(PostgresQueries.GET_ALL_SHARDS_QUERY)
        .rxExecute(Tuple.of(true))
        .map(ShardManagerClientImplPostgres::getShardDetailsFromRowSet)
        .doOnSuccess(
            shardDetails -> log.info("ShardManager:Active shards fetched: {}", shardDetails));
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMappingOrCreateDefault(String entityId) {
    return writerPgClient
        .rxBegin()
        .flatMap(
            tx ->
                tx.preparedQuery(PostgresQueries.GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY)
                    .rxExecute(Tuple.of(entityId))
                    .map(ShardManagerClientImplPostgres::getShardDetailsFromRowSet)
                    .flatMap(
                        shardDetails -> {
                          if (shardDetails.isEmpty()) {
                            return createDefaultEntityShardMapping(tx, entityId);
                          }
                          shardDetails.sort(Comparator.comparingLong(ShardDetails::getShardId));
                          EntityShardDetailsMapping entityShardDetailsMapping =
                              new EntityShardDetailsMapping(entityId, shardDetails);
                          log.debug(
                              "ShardManager:Found EntityShardMapping for entity {} with details:{}",
                              entityId,
                              shardDetails);
                          return Single.just(entityShardDetailsMapping);
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
    return writerPgClient
        .preparedQuery(PostgresQueries.GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY)
        .rxExecute(Tuple.of(entityId))
        .map(ShardManagerClientImplPostgres::getShardDetailsFromRowSet)
        .flatMap(
            shardDetails -> {
              if (shardDetails.isEmpty()) {
                return Single.error(new EntityNotMappedToShardException(entityId));
              } else {
                shardDetails.sort(Comparator.comparingLong(ShardDetails::getShardId));
                EntityShardDetailsMapping entityShardDetailsMapping =
                    new EntityShardDetailsMapping(entityId, shardDetails);
                log.debug(
                    "ShardManager:Found EntityShardMapping for entity {} with details:{}",
                    entityId,
                    shardDetails);
                return Single.just(entityShardDetailsMapping);
              }
            });
  }

  private Single<EntityShardDetailsMapping> createDefaultEntityShardMapping(
      Transaction tx, String entityId) {
    return tx.preparedQuery(PostgresQueries.GET_DEFAULT_SHARDS_QUERY)
        .rxExecute()
        .map(ShardManagerClientImplPostgres::getShardDetailsFromRowSet)
        .flatMap(
            defaultShards -> {
              if (defaultShards.isEmpty()) {
                return Single.error(new DefaultShardNotFoundException());
              }
              List<Long> defaultShardIds =
                  defaultShards.stream().map(ShardDetails::getShardId).collect(Collectors.toList());
              return tx.preparedQuery(PostgresQueries.CREATE_ENTITY_SHARD_MAPPING_QUERY)
                  .rxExecute(Tuple.of(entityId, new JsonArray(defaultShardIds)))
                  .map(res -> new EntityShardDetailsMapping(entityId, defaultShards));
            });
  }

  @Override
  public Single<List<EntityShardDetailsMapping>> rxListEntityShardMappings() {
    return readerPgClient
        .preparedQuery(PostgresQueries.GET_SHARD_DETAILS_FOR_ALL_ENTITIES_QUERY)
        .rxExecute()
        .map(ShardManagerClientImplPostgres::getEntityIdToActiveShardsMap)
        .map(
            entityIdToShardDetailsMap ->
                entityIdToShardDetailsMap.entrySet().stream()
                    .map(entry -> new EntityShardDetailsMapping(entry.getKey(), entry.getValue()))
                    .collect(Collectors.toList()));
  }

  @Override
  public Single<ShardManagerResponse> rxDeleteEntityShardMapping(String entityId) {
    return writerPgClient
        .preparedQuery(PostgresQueries.DELETE_ENTITY_SHARD_MAPPING_QUERY)
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
              return writerPgClient
                  .preparedQuery(PostgresQueries.CREATE_ENTITY_SHARD_MAPPING_QUERY)
                  .rxExecute(Tuple.of(entityId, new JsonArray(shardIds)));
            })
        .map(any -> new ShardManagerResponse(true, "Mapping created"));
  }

  @Override
  public Single<ShardUpdateResponse> rxUpdateExistingShardDetails(
      long shardId, ShardConfig details) {
    return writerPgClient
        .rxBegin()
        .flatMap(
            tx -> {
              // First get current details
              return tx.preparedQuery(PostgresQueries.GET_SHARD_DETAILS_QUERY)
                  .rxExecute(Tuple.of(shardId))
                  .flatMap(
                      currentResult -> {
                        List<ShardDetails> currentShards = getShardDetailsFromRowSet(currentResult);
                        if (currentShards.isEmpty()) {
                          return Single.error(
                              new IllegalArgumentException("Shard not found with id: " + shardId));
                        }
                        ShardDetails currentShard = currentShards.get(0);

                        // Then update the shard details
                        return tx.preparedQuery(PostgresQueries.SET_SHARD_DETAILS_QUERY)
                            .rxExecute(Tuple.of(JsonObject.mapFrom(details).toString(), shardId))
                            .map(
                                rows -> {
                                  if (rows.rowCount() == 0) {
                                    throw new IllegalArgumentException(
                                        "Failed to update shard with id: " + shardId);
                                  }
                                  List<ShardDetails> updatedShards =
                                      getShardDetailsFromRowSet(rows);
                                  if (updatedShards.isEmpty()) {
                                    throw new IllegalArgumentException(
                                        "Failed to parse updated shard details");
                                  }
                                  return ShardUpdateResponse.builder()
                                      .currentShardDetails(currentShard)
                                      .updatedShardDetails(updatedShards.get(0))
                                      .build();
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

  private static List<ShardDetails> getShardDetailsFromRowSet(RowSet<Row> rows)
      throws JsonProcessingException {
    List<ShardDetails> shardDetails = new ArrayList<>();
    for (Row row : rows) {
      long shardId = row.getLong("shardid");
      String shardMetaJson = row.getString("details");
      ShardConfig shardConfig = objectMapper.readValue(shardMetaJson, ShardConfig.class);
      shardDetails.add(new ShardDetails(shardId, shardConfig));
    }
    return shardDetails;
  }

  private static Map<String, List<ShardDetails>> getEntityIdToActiveShardsMap(RowSet<Row> rows) {
    Map<String, List<ShardDetails>> entityIdToShardDetailsMap = new HashMap<>();
    for (Row row : rows) {
      String entityId = row.getString("entityid");
      long shardId = row.getLong("shardid");
      JsonObject shardMetaJson = new JsonObject(row.getString("details"));
      ShardConfig shardConfig = shardMetaJson.mapTo(ShardConfig.class);
      ShardDetails shardDetails = new ShardDetails(shardId, shardConfig);
      entityIdToShardDetailsMap.computeIfAbsent(entityId, k -> new ArrayList<>()).add(shardDetails);
    }
    return entityIdToShardDetailsMap;
  }

  private void checkIfMasterInReadOnlyMode() {
    try {
      writerPgClient
          .query(SHOW_TRANSACTION_READ_ONLY)
          .rxExecute()
          .subscribe(
              this::handleSuccess,
              error ->
                  log.error(
                      "ShardManager:Error in getting readonly information from master", error));
    } catch (Exception e) {
      log.error("ShardManager:Error in getting readonly information from master", e);
    }
  }

  private void handleSuccess(RowSet<Row> rowSet) {
    if (rowSet.iterator().hasNext()) {
      String readOnly = rowSet.iterator().next().getString("transaction_read_only");
      log.debug("ShardManager:Readonly mode on masterHost:{}", readOnly);
      if ("on".equals(readOnly)) {
        log.info("ShardManager:current masterHost is not a master, reconnecting");
        rxClose().blockingAwait();
        createMasterSlaveConnection();
      }
    }
  }
}
