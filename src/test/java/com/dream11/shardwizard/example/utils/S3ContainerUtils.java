package com.dream11.shardwizard.example.utils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletionException;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

@Slf4j
public class S3ContainerUtils {

  private static final String LOCAL_ENDPOINT = "http://localhost:4566";
  private static final String DEFAULT_REGION = "us-east-1";
  private static final String RESOURCE_BASE_PATH = "src/test/resources";

  private final S3AsyncClient s3Client;

  public S3ContainerUtils() {
    this.s3Client = createDefaultClient();
  }

  private S3AsyncClient createDefaultClient() {
    return S3AsyncClient.builder()
        .endpointOverride(URI.create(LOCAL_ENDPOINT))
        .region(Region.of(DEFAULT_REGION))
        .credentialsProvider(
            StaticCredentialsProvider.create(AwsBasicCredentials.create("accesskey", "secretkey")))
        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
        .forcePathStyle(true)
        .build();
  }

  public void initializeTestEnvironment(S3Config config) throws Exception {
    log.info("Initializing S3 test environment with config: {}", config);

    // Create base structure
    createBucket(config.getBucketName());
    createFolder(config.getBucketName(), config.getBaseFolder());

    // Upload shard master configuration
    uploadShardMasterConfig(config);

    // Upload entity mappings
    uploadEntityMappings(config);
  }

  private void uploadShardMasterConfig(S3Config config) throws Exception {
    String shardMasterFile = config.getBaseFolder() + "/shardmaster.json";
    String sourcePath = Paths.get(RESOURCE_BASE_PATH, "s3", "shard-master.json").toString();
    uploadFileAtALocation(config.getBucketName(), shardMasterFile, sourcePath);
  }

  private void uploadEntityMappings(S3Config config) throws Exception {
    String mappingFolder = config.getBaseFolder() + "/entity_shard_mapping";
    createFolder(config.getBucketName(), mappingFolder);

    // Upload all round mapping files
    uploadRoundMappings(config.getBucketName(), mappingFolder);
  }

  private void uploadRoundMappings(String bucketName, String mappingFolder) throws Exception {
    Path mappingsDir = Paths.get(RESOURCE_BASE_PATH, "s3", "entity-mappings");
    Files.list(mappingsDir)
        .filter(path -> path.toString().endsWith(".json"))
        .forEach(
            path -> {
              try {
                String fileName = path.getFileName().toString();
                uploadFileAtALocation(bucketName, mappingFolder + "/" + fileName, path.toString());
              } catch (Exception e) {
                throw new RuntimeException("Failed to upload mapping file: " + path, e);
              }
            });
  }

  public CreateBucketResponse createBucket(String bucketName) {
    log.debug("Creating bucket: {}", bucketName);
    CreateBucketRequest request = CreateBucketRequest.builder().bucket(bucketName).build();
    return s3Client.createBucket(request).join();
  }

  public PutObjectResponse createFolder(String bucketName, String folderName) {
    log.debug("Creating folder: {}/{}", bucketName, folderName);
    PutObjectRequest request =
        PutObjectRequest.builder().bucket(bucketName).key(folderName + "/").build();

    try {
      return s3Client.putObject(request, AsyncRequestBody.fromString("")).join();
    } catch (CompletionException e) {
      throw unwrapCompletionException(e, "Error creating folder: " + folderName);
    }
  }

  public PutObjectResponse uploadFile(String bucketName, String fileName, String content) {
    log.debug("Uploading file: {}/{}", bucketName, fileName);
    PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(fileName).build();
    return s3Client
        .putObject(request, AsyncRequestBody.fromString(content, StandardCharsets.UTF_8))
        .join();
  }

  public PutObjectResponse uploadFileAtALocation(
      String bucketName, String fileName, String filePath) throws Exception {
    log.debug("Uploading file from path: {} to {}/{}", filePath, bucketName, fileName);
    String content = new String(Files.readAllBytes(Paths.get(filePath)), StandardCharsets.UTF_8);
    return uploadFile(bucketName, fileName, content);
  }

  public ListObjectsV2Response listObjects(String bucketName) {
    log.debug("Listing objects in bucket: {}", bucketName);
    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketName).build();
    return s3Client.listObjectsV2(request).join();
  }

  private RuntimeException unwrapCompletionException(CompletionException e, String message) {
    Throwable cause = e.getCause();
    if (cause != null) {
      return new RuntimeException(message + ": " + cause.getMessage(), cause);
    }
    return e;
  }

  @Builder
  @Data
  public static class S3Config {

    private final String accessKey;
    private final String secretKey;
    private final String region;
    private final URI endpoint;
    private final String bucketName;
    private final String baseFolder;
  }
}
