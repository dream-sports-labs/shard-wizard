package com.dream11.shardwizard.shardmanager.impl.s3;

import com.dream11.shardwizard.config.S3Config;
import com.dream11.shardwizard.exception.EntityNotMappedToShardException;
import com.dream11.shardwizard.model.EntityShardDetailsMapping;
import com.dream11.shardwizard.model.EntityShardMappingS3;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardDetails;
import com.dream11.shardwizard.model.ShardDetailsS3;
import com.dream11.shardwizard.model.ShardManagerResponse;
import com.dream11.shardwizard.shardmanager.ShardManagerClient;
import com.dream11.shardwizard.shardmanager.impl.s3.credentials.S3ClientStrategy;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

@Slf4j
public class ShardManagerClientImplS3 implements ShardManagerClient {

  private final String bucketName;
  private final String shardMasterFile;
  private final String entityShardMappingFolder;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final S3Config s3Config;
  private final S3ClientStrategy s3ClientStrategy;
  private S3AsyncClient s3Client;

  public ShardManagerClientImplS3(
      Vertx vertx, S3Config s3Config, S3ClientStrategy s3ClientStrategy) {
    this.bucketName = s3Config.getBucketName();
    this.shardMasterFile = s3Config.getShardMasterFilePath();
    this.entityShardMappingFolder = s3Config.getEntityShardMappingFolderPath();
    this.s3Config = s3Config;
    this.s3ClientStrategy = s3ClientStrategy;
  }

  @Override
  public Completable rxBootstrap() {
    this.s3Client = s3ClientStrategy.createClient(s3Config);
    log.info("Bootstrapping ShardManagerClientImplS3...");
    return Completable.complete();
  }

  @Override
  public Completable rxClose() {
    log.info("Closing S3 client...");
    s3Client.close();
    return Completable.complete();
  }

  @Override
  public Single<ShardDetails> rxRegisterNewShard(boolean isDefault, ShardConfig details) {
    log.info("Registering new shard with default: {}, details: {}", isDefault, details);
    return Single.fromFuture(
        readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class)
            .thenCompose(existingList -> registerNewShard(existingList, isDefault, details)));
  }

  @Override
  public Single<ShardManagerResponse> rxEstablishEntityToShardsMapping(
      String entityId, List<Long> shardIds) {
    EntityShardMappingS3 mapping = new EntityShardMappingS3(entityId, shardIds);
    String key = entityShardMappingFolder + entityId + ".json";

    log.info("Establishing shard mapping for entityId: {} with shardIds: {}", entityId, shardIds);

    return Single.fromFuture(
        writeJsonToS3(key, mapping)
            .thenApply(
                v -> {
                  log.info("Successfully wrote mapping for entityId: {}", entityId);
                  return new ShardManagerResponse(
                      true, "Mapping successfully written for entity: " + entityId);
                })
            .exceptionally(
                ex -> {
                  log.error("Failed to write mapping for entityId: {}", entityId, ex);
                  return new ShardManagerResponse(
                      false, "Failed to write mapping: " + ex.getMessage());
                }));
  }

  @Override
  public Single<ShardManagerResponse> rxSetDefaultFlag(List<Long> shardIds, boolean isDefault) {
    log.info("Setting default flag to {} for shard IDs: {}", isDefault, shardIds);
    return Single.fromFuture(
        readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class)
            .thenCompose(allShards -> updateDefaultFlag(allShards, shardIds, isDefault)));
  }

  @Override
  public Single<ShardManagerResponse> rxDeactivateShard(long shardId) {
    log.info("Deactivating shard ID: {}", shardId);
    return Single.fromFuture(
        readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class)
            .thenCompose(allShards -> deactivateShard(allShards, shardId)));
  }

  @Override
  public Single<List<ShardDetails>> rxGetActiveShards() {
    return Single.fromFuture(
            readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class)
                .thenApply(this::filterActiveShards))
        .doOnSuccess(activeShards -> log.info("Active shards fetched: {}", activeShards));
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMappingOrCreateDefault(String entityId) {
    return rxFindMapping(entityId)
        .onErrorResumeNext(
            err -> {
              log.warn("Mapping not found for entityId: {}. Creating default mapping.", entityId);
              return Single.fromFuture(
                  readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class)
                      .thenCompose(allShards -> createDefaultMapping(entityId, allShards)));
            });
  }

  @Override
  public Single<EntityShardDetailsMapping> rxFindMapping(String entityId) {
    String mappingKey = entityShardMappingFolder + entityId + ".json";
    log.info("Attempting to find shard mapping for entityId: {}", entityId);
    return Single.fromFuture(
            fileExists(mappingKey)
                .thenCompose(
                    exists -> {
                      if (!exists) {
                        throw new EntityNotMappedToShardException(entityId);
                      }
                      return getObjectAsString(mappingKey).thenApply(this::parseEntityShardMapping);
                    })
                .thenCombine(
                    readJsonListFromS3(shardMasterFile, ShardDetailsS3[].class),
                    this::resolveShardDetails))
        .onErrorResumeNext(
            error -> {
              Throwable cause = (error instanceof ExecutionException) ? error.getCause() : error;
              log.error("Failed to find mapping for entityId: {}", entityId, cause);
              return Single.error(cause);
            });
  }

  @Override
  public Single<List<EntityShardDetailsMapping>> rxListEntityShardMappings() {
    log.info("Listing all entity-shard mappings from folder: {}", entityShardMappingFolder);
    return Single.just(new ArrayList<>());
  }

  @Override
  public Single<ShardManagerResponse> rxDeleteEntityShardMapping(String entityId) {
    String key = entityShardMappingFolder + entityId + ".json";
    log.info("Attempting to delete entity-shard mapping for entityId: {}", entityId);
    return Single.fromFuture(
        fileExists(key)
            .thenCompose(
                exists -> {
                  if (!exists) {
                    log.info("No mapping exists for entityId: {}. Nothing to delete.", entityId);
                    return CompletableFuture.completedFuture(
                        new ShardManagerResponse(
                            false, "Entity mapping does not exist for: " + entityId));
                  }
                  return deleteMappingFromS3(key, entityId);
                }));
  }

  private CompletableFuture<ShardDetails> registerNewShard(
      List<ShardDetailsS3> existingList, boolean isDefault, ShardConfig details) {
    long nextShardId =
        existingList.stream().mapToLong(ShardDetailsS3::getShardId).max().orElse(0L) + 1;
    log.info("Assigning new shard ID: {}", nextShardId);
    ShardDetailsS3 newShard = new ShardDetailsS3(nextShardId, true, isDefault, details);
    existingList.add(newShard);
    return writeJsonToS3(shardMasterFile, existingList)
        .thenApply(
            v -> {
              log.info("Successfully registered shard ID: {}", newShard.getShardId());
              return new ShardDetails(newShard.getShardId(), newShard.getDetails());
            });
  }

  private CompletableFuture<ShardManagerResponse> updateDefaultFlag(
      List<ShardDetailsS3> allShards, List<Long> shardIds, boolean isDefault) {
    allShards.forEach(
        shard -> {
          if (shardIds.contains(shard.getShardId())) {
            shard.setDefault(isDefault);
          }
        });
    return writeJsonToS3(shardMasterFile, allShards)
        .thenApply(v -> new ShardManagerResponse(true, "Default flag updated"));
  }

  private CompletableFuture<ShardManagerResponse> deactivateShard(
      List<ShardDetailsS3> allShards, long shardId) {
    allShards.forEach(
        shard -> {
          if (shard.getShardId() == shardId) {
            shard.setActive(false);
          }
        });
    return writeJsonToS3(shardMasterFile, allShards)
        .thenApply(v -> new ShardManagerResponse(true, "Shard deactivated: " + shardId));
  }

  private List<ShardDetails> filterActiveShards(List<ShardDetailsS3> allShards) {
    return allShards.stream()
        .filter(ShardDetailsS3::isActive)
        .map(s -> new ShardDetails(s.getShardId(), s.getDetails()))
        .collect(Collectors.toList());
  }

  private EntityShardMappingS3 parseEntityShardMapping(String json) {
    try {
      return objectMapper.readValue(json, EntityShardMappingS3.class);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse entity shard mapping", e);
    }
  }

  private EntityShardDetailsMapping resolveShardDetails(
      EntityShardMappingS3 s3Mapping, List<ShardDetailsS3> allShards) {
    Map<Long, ShardDetails> shardMap =
        allShards.stream()
            .collect(
                Collectors.toMap(
                    ShardDetailsS3::getShardId,
                    s -> new ShardDetails(s.getShardId(), s.getDetails())));
    List<ShardDetails> matchedShards =
        s3Mapping.getShardIds().stream()
            .map(shardMap::get)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    log.info(
        "Resolved {} shard(s) for entityId: {}", matchedShards.size(), s3Mapping.getEntityId());
    return new EntityShardDetailsMapping(s3Mapping.getEntityId(), matchedShards);
  }

  private CompletableFuture<ShardManagerResponse> deleteMappingFromS3(String key, String entityId) {
    DeleteObjectRequest request = DeleteObjectRequest.builder().bucket(bucketName).key(key).build();
    return s3Client
        .deleteObject(request)
        .thenApply(
            response -> {
              log.info("Successfully deleted entity-shard mapping for entityId: {}", entityId);
              return new ShardManagerResponse(true, "Entity mapping deleted successfully");
            });
  }

  private CompletableFuture<EntityShardDetailsMapping> createDefaultMapping(
      String entityId, List<ShardDetailsS3> allShards) {
    List<ShardDetails> defaultShards =
        allShards.stream()
            .filter(ShardDetailsS3::isActive)
            .filter(ShardDetailsS3::isDefault)
            .sorted(Comparator.comparingLong(ShardDetailsS3::getShardId))
            .map(s -> new ShardDetails(s.getShardId(), s.getDetails()))
            .collect(Collectors.toList());
    log.info(
        "{} default active shard(s) found for new mapping of entityId: {}",
        defaultShards.size(),
        entityId);
    EntityShardDetailsMapping mapping = new EntityShardDetailsMapping(entityId, defaultShards);
    EntityShardMappingS3 s3Format =
        new EntityShardMappingS3(
            entityId,
            defaultShards.stream().map(ShardDetails::getShardId).collect(Collectors.toList()));
    return writeJsonToS3(entityShardMappingFolder + entityId + ".json", s3Format)
        .thenApply(
            v -> {
              log.info("Default mapping created and saved for entityId: {}", entityId);
              return mapping;
            });
  }

  public CompletableFuture<Boolean> fileExists(String key) {
    HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucketName).key(key).build();

    return s3Client
        .headObject(request)
        .thenApply(response -> true)
        .exceptionally(
            ex -> {
              if (ex.getCause() instanceof NoSuchKeyException
                  || ex.getMessage().contains("Not Found")) {
                return false;
              }
              throw new CompletionException(ex); // rethrow unexpected
            });
  }

  private <T> CompletableFuture<List<T>> readJsonListFromS3(String key, Class<T[]> clazzArray) {
    return getObjectAsString(key)
        .thenApply(
            json -> {
              try {
                if (json == null || json.trim().isEmpty()) {
                  return new ArrayList<>();
                }
                T[] array = objectMapper.readValue(json, clazzArray);
                return new ArrayList<>(Arrays.asList(array));
              } catch (Exception e) {
                throw new RuntimeException("Failed to parse list from: " + key, e);
              }
            });
  }

  private CompletableFuture<Void> writeJsonToS3(String key, Object data) {
    try {
      String json = objectMapper.writeValueAsString(data);
      log.info("Writing JSON to S3: {}", json);
      PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(key).build();
      return s3Client.putObject(request, AsyncRequestBody.fromString(json)).thenAccept(r -> {});
    } catch (Exception e) {
      CompletableFuture<Void> failed = new CompletableFuture<>();
      failed.completeExceptionally(new RuntimeException("Failed to write JSON to: " + key, e));
      return failed;
    }
  }

  private CompletableFuture<String> getObjectAsString(String key) {
    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(key).build();

    return s3Client
        .getObject(request, AsyncResponseTransformer.toBytes())
        .thenApply(resp -> resp.asUtf8String());
  }
}
