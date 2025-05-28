package com.dream11.shardwizard.dao;

import static com.dream11.shardwizard.constant.Constants.REFRESH_SHARDS_TIMEOUT_SECONDS;
import static com.dream11.shardwizard.constant.Constants.SHARD_MANAGER_CONFIG_FOLDER;

import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.exception.DatabaseTypeNotFoundException;
import com.dream11.shardwizard.exception.ShardNotPresentException;
import com.dream11.shardwizard.model.EntityShardDetailsMapping;
import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.dream11.shardwizard.model.ShardDetails;
import com.dream11.shardwizard.router.ShardRouter;
import com.dream11.shardwizard.router.impl.ModuloRouter;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.dream11.shardwizard.utils.CompletableFutureUtils;
import com.dream11.shardwizard.utils.ConfigUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.Future;
import io.vertx.reactivex.SingleHelper;
import io.vertx.reactivex.core.Vertx;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;

@Slf4j
public abstract class AbstractDaoFactory<T> implements DaoFactory<T> {

  protected final Vertx vertx;
  private final ConcurrentHashMap<Long, CompletableFuture<T>> shardIdToDaoCache;
  private final ConcurrentHashMap<String, CompletableFuture<EntityShardDetailsMapping>>
      entityIdToShardMappingCache;
  private final ConcurrentHashMap<String, ShardRouter> entityToShardRouterCache;
  private final ShardManagerClient shardManagerClient;
  private final ScheduledExecutorService executorService;
  protected ShardManagerConfig shardManagerConfig;

  protected AbstractDaoFactory(Vertx vertx) {
    this.vertx = vertx;
    this.shardManagerConfig =
        ConfigUtils.fromConfigFile(
            "config/" + SHARD_MANAGER_CONFIG_FOLDER + "/%s.conf", ShardManagerConfig.class);
    this.shardIdToDaoCache = new ConcurrentHashMap<>();
    this.entityIdToShardMappingCache = new ConcurrentHashMap<>();
    this.shardManagerClient = ShardManagerClient.create(vertx);
    this.executorService = Executors.newSingleThreadScheduledExecutor();
    this.entityToShardRouterCache = new ConcurrentHashMap<>();
  }

  @NonNull
  protected abstract T getDaoImplFromType(ShardDetails shardDetails);

  protected abstract long getShardIdFromPrimaryKey(String primaryKey);

  protected ShardRouter getShardRouter() {
    return new ModuloRouter();
  }

  private Single<T> rxGetAndConnectDaoInstance(ShardDetails shardDetails) {
    return Single.just(1)
        .map(
            any -> {
              ShardDetails finalShardDetails = updateShardConnectionParams(shardDetails);
              return getDaoImplFromType(finalShardDetails);
            })
        .flatMap(daoInstance -> rxCreateConnections(daoInstance).toSingleDefault(daoInstance));
  }

  private ShardConnectionParameters mergeConfigs(
      ShardConnectionParameters pojo1,
      ShardConnectionParameters pojo2,
      Class<ShardConnectionParameters> kclass) {
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    try {
      String json1 = objectMapper.writeValueAsString(pojo1);
      String json2 = objectMapper.writeValueAsString(pojo2);
      Config config1 = ConfigFactory.parseString(json1);
      Config config2 = ConfigFactory.parseString(json2);
      Config finalConfig = config1.withFallback(config2).resolve();
      return ConfigBeanFactory.create(finalConfig, kclass);
    } catch (IOException e) {
      throw new RuntimeException("Error merging POJOs to Config", e);
    }
  }

  private ShardDetails updateShardConnectionParams(ShardDetails shardDetails) {
    String sourceType = shardManagerConfig.getSourceType();
    String databaseType = shardDetails.getShardConfig().getDatabaseType().name();
    ShardConnectionParameters defaultConnectionParams =
        shardManagerConfig
            .convertToSourceConfig(sourceType)
            .getParsedShardConfigMap()
            .get(databaseType);
    ShardConnectionParameters connectionParamsFromDB =
        shardDetails.getShardConfig().getShardConnectionParams();
    ShardConnectionParameters mergedConnectionParams =
        mergeConfigs(
            connectionParamsFromDB, defaultConnectionParams, ShardConnectionParameters.class);
    shardDetails.getShardConfig().setShardConnectionParams(mergedConnectionParams);
    return shardDetails;
  }

  @Override
  public Single<List<ShardDetails>> rxGetOrCreateEntityShardDetails(String entityId)
      throws ShardNotPresentException {
    return CompletableFutureUtils.toSingle(
        entityIdToShardMappingCache
            .computeIfAbsent(
                entityId,
                any ->
                    getOrCreateEntityShardMappingFromDB(any)
                        .whenComplete(
                            (esm, err) -> {
                              if (Objects.nonNull(err)) {
                                log.error(
                                    "Error while fetching shard mapping for entityId: {}",
                                    entityId,
                                    err);
                                entityIdToShardMappingCache.remove(entityId);
                                throw new RuntimeException(
                                    "Error while fetching shard mapping for entityId: " + entityId);
                              }
                            }))
            .thenApply(this::getShardDetailsFromDao));
  }

  @Override
  public Single<List<ShardDetails>> rxGetEntityShardDetails(String entityId) {
    return CompletableFutureUtils.toSingle(
        entityIdToShardMappingCache
            .computeIfAbsent(
                entityId,
                any ->
                    getEntityShardMappingFromDB(any)
                        .whenComplete(
                            (esm, err) -> {
                              if (Objects.nonNull(err)) {
                                log.error(
                                    "Error while fetching shard mapping for entityId: {}",
                                    entityId,
                                    err);
                                entityIdToShardMappingCache.remove(entityId);
                                throw new RuntimeException(err);
                              }
                            }))
            .thenApply(this::getShardDetailsFromDao));
  }

  private List<ShardDetails> getShardDetailsFromDao(EntityShardDetailsMapping esm) {
    return esm.getActiveShards().stream()
        .map(
            shardDetails -> {
              try {
                CompletableFuture<T> daoFuture = shardIdToDaoCache.get(shardDetails.getShardId());
                if (Objects.isNull(daoFuture)) {
                  throw new ShardNotPresentException(shardDetails.getShardId());
                }
                BaseDaoAbstract dao =
                    (BaseDaoAbstract) shardIdToDaoCache.get(shardDetails.getShardId()).get();
                return dao.getShardDetails();
              } catch (RuntimeException | InterruptedException | ExecutionException e) {
                throw new ShardNotPresentException(shardDetails.getShardId());
              }
            })
        .collect(Collectors.toList());
  }

  @Override
  public Single<List<T>> rxGetAllActiveShardsDAOs() {
    List<Single<T>> singles =
        shardIdToDaoCache.values().stream()
            .map(CompletableFutureUtils::toSingle)
            .collect(Collectors.toList());
    return Single.merge(singles).toList();
  }

  @Override
  public Single<T> rxGetOrCreateEntityShardDao(String entityId, long routeKey) {
    return rxGetOrCreateEntityShardDao(entityId, String.valueOf(routeKey));
  }

  @Override
  public Single<T> rxGetOrCreateEntityShardDao(String entityId, String routeKey) {
    return CompletableFutureUtils.toSingle(
            entityIdToShardMappingCache.computeIfAbsent(
                entityId,
                any ->
                    getOrCreateEntityShardMappingFromDB(any)
                        .whenComplete(
                            (esm, err) -> {
                              if (Objects.nonNull(err)) {
                                log.error(
                                    "Error while fetching shard mapping for entityId: {}",
                                    entityId,
                                    err);
                                entityIdToShardMappingCache.remove(entityId);
                              }
                              log.info("Fetched shard mapping for entityId: {}", entityId);
                            })))
        .flatMap(entityShardMapping -> rxGetOrCreateEntityShardDao(routeKey, entityShardMapping));
  }

  public Single<T> rxGetDaoInstanceByPrimaryKey(String primaryKey) {
    return CompletableFutureUtils.toSingle(
        shardIdToDaoCache.get(getShardIdFromPrimaryKey(primaryKey)));
  }

  @Override
  public Completable rxBootstrap() {
    return shardManagerClient
        .rxBootstrap()
        .doOnComplete(
            () -> {
              log.info("ShardManagerClient bootstrapped successfully");
            })
        .andThen(
            Completable.defer(
                () ->
                    shardManagerClient
                        .rxGetActiveShards()
                        .map(
                            any -> {
                              log.info("Creating connections to shards");
                              return any;
                            })
                        .flatMapCompletable(this::bootstrapConnections)
                        .doOnComplete(() -> schedulePeriodicShardsRefresh(shardManagerClient))));
  }

  private Completable bootstrapConnections(List<ShardDetails> shardDetailsList) {
    return CompletableFutureUtils.toSingle(
            CompletableFuture.allOf(
                shardDetailsList.stream()
                    .map(
                        shardDetails -> {
                          Single<T> daoSingle =
                              rxGetAndConnectDaoInstance(shardDetails)
                                  .doOnSuccess(
                                      x ->
                                          log.info(
                                              "Connected to shardId: {}",
                                              shardDetails.getShardId()));
                          VertxCompletableFuture<T> future =
                              CompletableFutureUtils.fromSingle(daoSingle);
                          shardIdToDaoCache.put(shardDetails.getShardId(), future);
                          return future;
                        })
                    .toArray(CompletableFuture[]::new)))
        .ignoreElement();
  }

  private CompletableFuture<EntityShardDetailsMapping> getOrCreateEntityShardMappingFromDB(
      String entityId) {
    Single<EntityShardDetailsMapping> single =
        shardManagerClient.rxFindMappingOrCreateDefault(entityId);
    return CompletableFutureUtils.fromSingle(single);
  }

  private CompletableFuture<EntityShardDetailsMapping> getEntityShardMappingFromDB(
      String entityId) {
    Single<EntityShardDetailsMapping> single = shardManagerClient.rxFindMapping(entityId);
    return CompletableFutureUtils.fromSingle(single);
  }

  private void schedulePeriodicShardsRefresh(ShardManagerClient shardManagerClient) {
    executorService.scheduleAtFixedRate(
        () -> refreshCaches(shardManagerClient),
        10,
        shardManagerConfig.getShardsRefreshSeconds(),
        TimeUnit.SECONDS);
  }

  private void refreshCaches(ShardManagerClient shardManagerClient) {
    try {
      rxRefreshShards(shardManagerClient)
          .doOnComplete(() -> log.debug("Shards refreshed successfully"))
          .timeout(REFRESH_SHARDS_TIMEOUT_SECONDS, TimeUnit.SECONDS)
          .blockingAwait();
    } catch (Exception e) {
      log.error("Error occurred while refreshing shards", e);
    }
  }

  private Completable rxRefreshShards(ShardManagerClient shardManagerClient) {
    return shardManagerClient
        .rxGetActiveShards()
        .flatMapCompletable(this::rxCloseRemovedAndConnectNewShards);
  }

  private Completable rxCloseRemovedAndConnectNewShards(List<ShardDetails> activeShardsDetails) {
    return Single.just(1)
        .flatMap(
            any -> {
              closeConnectionsOfRemovedShardsAsync(activeShardsDetails);

              CompletableFuture<Void> future =
                  CompletableFuture.allOf(
                      activeShardsDetails.stream()
                          .map(
                              shardDetails ->
                                  shardIdToDaoCache
                                      .computeIfAbsent(
                                          shardDetails.getShardId(),
                                          key -> connectNewFoundShard(shardDetails))
                                      .whenComplete(
                                          (dao, err) -> {
                                            if (Objects.nonNull(err)) {
                                              log.error(
                                                  "Error while connecting to shardId: {}",
                                                  shardDetails.getShardId(),
                                                  err);
                                              shardIdToDaoCache.remove(shardDetails.getShardId());
                                            }
                                          }))
                          .toArray(CompletableFuture[]::new));

              return SingleHelper.<Integer>toSingle(
                  (asyncResultHandler) -> {
                    future.whenComplete(
                        (v, err) -> {
                          if (err != null) {
                            asyncResultHandler.handle(Future.failedFuture(err));
                          } else {
                            asyncResultHandler.handle(Future.succeededFuture(1));
                          }
                        });
                  });
            })
        .ignoreElement();
  }

  private CompletableFuture<T> connectNewFoundShard(ShardDetails shardDetails) {
    log.info("Found new shard. Connecting to shardId: {}", shardDetails.getShardId());
    CompletableFuture<T> future = new CompletableFuture<>();
    rxGetAndConnectDaoInstance(shardDetails)
        .subscribe(future::complete, future::completeExceptionally);
    return future;
  }

  private void closeConnectionsOfRemovedShardsAsync(List<ShardDetails> activeShardsDetails) {
    Set<Long> activeShardIds =
        activeShardsDetails.stream().map(ShardDetails::getShardId).collect(Collectors.toSet());

    List<Entry<Long, CompletableFuture<T>>> removedShards =
        shardIdToDaoCache.entrySet().stream()
            .filter(entry -> !activeShardIds.contains(entry.getKey()))
            .collect(Collectors.toList());
    if (removedShards.isEmpty()) {
      return;
    }

    List<Long> removedShardIds =
        removedShards.stream().map(Entry::getKey).collect(Collectors.toList());
    log.info("Found removed shards. Closing connections of these shardIds: {}", removedShardIds);
    removedShards.forEach(
        entry -> {
          Long shardId = entry.getKey();
          CompletableFuture<T> futureDao = entry.getValue();

          Single.fromFuture(futureDao)
              .flatMapCompletable(this::rxCloseConnections)
              .doOnComplete(
                  () -> {
                    shardIdToDaoCache.remove(shardId);
                    log.info("Successfully closed and removed shardId: {}", shardId);
                  })
              .doOnError(
                  err -> log.error("Error while closing connection for shardId: {}", shardId, err))
              .subscribe(); // fire-and-forget
        });
  }

  private Completable rxCreateConnections(T daoInstance) {
    if (daoInstance instanceof BaseDaoAbstract) {
      return ((BaseDaoAbstract) daoInstance).rxConnect();
    }
    return Completable.error(new DatabaseTypeNotFoundException());
  }

  private Completable rxCloseConnections(T daoInstance) {
    return Single.just(1)
        .flatMapCompletable(
            any -> {
              if (daoInstance instanceof BaseDaoAbstract) {
                return ((BaseDaoAbstract) daoInstance).rxCloseConnections();
              }
              return Completable.error(new DatabaseTypeNotFoundException());
            });
  }

  public Completable rxClose() {
    return Completable.fromAction(
            () -> {
              log.info("Closing shardsRefresh executor service");
              executorService.shutdownNow();
            })
        .andThen(
            Completable.defer(
                () -> {
                  List<Completable> completablesList =
                      shardIdToDaoCache.values().stream()
                          .map(
                              future -> {
                                // note: cannot use CompletableFutureUtils.toSingle() here since,
                                // this function will be called in application termination where
                                // vertx context doesn't exist. This causes null pointer exception.
                                // Although Single.fromFuture() is blocking, but it's okay since
                                // it's app termination
                                return Single.fromFuture(future)
                                    .flatMapCompletable(this::rxCloseConnections);
                              })
                          .collect(Collectors.toList());

                  completablesList.add(Completable.defer(shardManagerClient::rxClose));
                  return Completable.mergeDelayError(completablesList);
                }));
  }

  private Single<T> rxGetOrCreateEntityShardDao(
      String routeKey, EntityShardDetailsMapping entityShardDetailsMapping) {
    return Single.just(1)
        .flatMap(
            any -> {
              ShardDetails shardDetails = getRoutedShard(routeKey, entityShardDetailsMapping);
              CompletableFuture<T> fut = shardIdToDaoCache.get(shardDetails.getShardId());
              if (Objects.nonNull(fut)) {
                return CompletableFutureUtils.toSingle(fut);
              } else {
                return Single.error(new ShardNotPresentException(shardDetails.getShardId()));
              }
            });
  }

  @NonNull
  private ShardDetails getRoutedShard(
      String routeKey, EntityShardDetailsMapping entityShardDetailsMapping) {
    ShardRouter router =
        getOrCreateShardRouter(
            entityShardDetailsMapping.getEntityId(), entityShardDetailsMapping.getActiveShards());
    long routedShardId = router.getRoutedShardId(routeKey);
    return entityShardDetailsMapping.getActiveShards().stream()
        .filter(shardDetails -> shardDetails.getShardId() == routedShardId)
        .findFirst()
        .orElseThrow(() -> new IllegalStateException("No active shards found for entity"));
  }

  private ShardRouter getOrCreateShardRouter(String entityId, List<ShardDetails> shards) {
    return entityToShardRouterCache.computeIfAbsent(
        entityId,
        key -> {
          ShardRouter router = getShardRouter();
          List<Long> shardIds =
              shards.stream().map(ShardDetails::getShardId).collect(Collectors.toList());
          router.initialize(shardIds);
          return router;
        });
  }
}
