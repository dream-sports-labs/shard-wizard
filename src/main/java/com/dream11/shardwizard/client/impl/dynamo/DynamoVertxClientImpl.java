package com.dream11.shardwizard.client.impl.dynamo;

import com.dream11.shardwizard.circuitbreaker.client.AbstractCircuitBreakerClient;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultSingle;
import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;
import software.amazon.awssdk.http.crt.TcpKeepAliveConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

@Slf4j
public class DynamoVertxClientImpl extends AbstractCircuitBreakerClient
    implements DynamoVertxClient {

  private final Vertx vertx;
  private final String endpoint;
  private final String region;
  private final String accessKey;
  private final String secretKey;
  private DynamoDbAsyncClient dynamoClient;

  public DynamoVertxClientImpl(
      Vertx vertx,
      String endpoint,
      String region,
      String accessKey,
      String secretKey,
      ShardDetails shardDetails) {
    super(shardDetails);
    this.vertx = vertx;
    this.endpoint = endpoint;
    this.region = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  @Override
  public Completable rxConnect() {
    return withCircuitBreaker(
        vertx
            .rxExecuteBlocking(
                promise -> {
                  try {
                    dynamoClient = buildDynamoDbAsyncClient(region, endpoint, accessKey, secretKey);
                    log.info("✅ Successfully connected to DynamoDB");
                    promise.complete();
                  } catch (Exception e) {
                    log.error("❌ Failed to connect to DynamoDB", e);
                    promise.fail(e);
                  }
                })
            .ignoreElement());
  }

  private DynamoDbAsyncClient buildDynamoDbAsyncClient(
      String region, String endpoint, String accessKey, String secretKey) {

    // TODO - To move it to configuration.
    DynamoDbAsyncClientBuilder builder =
        DynamoDbAsyncClient.builder()
            .region(Region.of(region))
            .httpClientBuilder(
                AwsCrtAsyncHttpClient.builder()
                    .connectionTimeout(Duration.ofMillis(1000))
                    .connectionMaxIdleTime(Duration.ofMillis(15000))
                    .maxConcurrency(5000)
                    .tcpKeepAliveConfiguration(
                        TcpKeepAliveConfiguration.builder()
                            .keepAliveInterval(Duration.ofMillis(150))
                            .keepAliveTimeout(Duration.ofMillis(500))
                            .build()));

    if (accessKey != null && !accessKey.isBlank() && secretKey != null && !secretKey.isBlank()) {
      builder.credentialsProvider(
          StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)));
    }

    if (endpoint != null && !endpoint.trim().isEmpty()) {
      builder.endpointOverride(URI.create(endpoint));
    }

    return builder.build();
  }

  @Override
  public Single<Boolean> rxWriteItem(String tableName, Map<String, AttributeValue> item) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              dynamoClient
                  .putItem(PutItemRequest.builder().tableName(tableName).item(item).build())
                  .whenComplete(
                      (response, throwable) -> {
                        context.runOnContext(
                            v -> {
                              if (throwable != null) {
                                handler.handle(io.vertx.core.Future.failedFuture(throwable));
                              } else {
                                handler.handle(io.vertx.core.Future.succeededFuture(true));
                              }
                            });
                      });
            }));
  }

  @Override
  public Single<Boolean> rxUpdateItem(
      String tableName,
      Map<String, AttributeValue> key,
      Map<String, AttributeValueUpdate> updates) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              UpdateItemRequest request =
                  UpdateItemRequest.builder()
                      .tableName(tableName)
                      .key(key)
                      .attributeUpdates(updates)
                      .build();

              dynamoClient
                  .updateItem(request)
                  .whenComplete(
                      (response, throwable) -> {
                        context.runOnContext(
                            v -> {
                              if (throwable != null) {
                                handler.handle(io.vertx.core.Future.failedFuture(throwable));
                              } else {
                                handler.handle(io.vertx.core.Future.succeededFuture(true));
                              }
                            });
                      });
            }));
  }

  @Override
  public Single<Boolean> rxDeleteItem(String tableName, Map<String, AttributeValue> key) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              DeleteItemRequest request =
                  DeleteItemRequest.builder().tableName(tableName).key(key).build();

              dynamoClient
                  .deleteItem(request)
                  .whenComplete(
                      (response, throwable) -> {
                        context.runOnContext(
                            v -> {
                              if (throwable != null) {
                                handler.handle(io.vertx.core.Future.failedFuture(throwable));
                              } else {
                                handler.handle(io.vertx.core.Future.succeededFuture(true));
                              }
                            });
                      });
            }));
  }

  @Override
  public Single<Map<String, AttributeValue>> rxGetItem(
      String tableName, Map<String, AttributeValue> key) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              dynamoClient
                  .getItem(GetItemRequest.builder().tableName(tableName).key(key).build())
                  .whenComplete(
                      (response, throwable) -> {
                        context.runOnContext(
                            v -> {
                              if (throwable != null) {
                                handler.handle(io.vertx.core.Future.failedFuture(throwable));
                              } else {
                                handler.handle(
                                    io.vertx.core.Future.succeededFuture(response.item()));
                              }
                            });
                      });
            }));
  }

  @Override
  public Single<Boolean> rxBatchWriteItem(
      String tableName, List<Map<String, AttributeValue>> itemsBatch) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              List<WriteRequest> writeRequests =
                  itemsBatch.stream()
                      .map(
                          item ->
                              WriteRequest.builder()
                                  .putRequest(PutRequest.builder().item(item).build())
                                  .build())
                      .collect(Collectors.toList());

              Map<String, List<WriteRequest>> requestItems = Map.of(tableName, writeRequests);

              BatchWriteItemRequest request =
                  BatchWriteItemRequest.builder().requestItems(requestItems).build();

              dynamoClient
                  .batchWriteItem(request)
                  .whenComplete(
                      (response, throwable) -> {
                        context.runOnContext(
                            v -> {
                              if (throwable != null) {
                                handler.handle(io.vertx.core.Future.failedFuture(throwable));
                              } else {
                                handler.handle(io.vertx.core.Future.succeededFuture(true));
                              }
                            });
                      });
            }));
  }

  @Override
  public Single<Boolean> rxTransactWriteItems(List<TransactWriteItem> items) {
    Context context = Vertx.currentContext();

    TransactWriteItemsRequest request =
        TransactWriteItemsRequest.builder().transactItems(items).build();

    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler ->
                dynamoClient
                    .transactWriteItems(request)
                    .whenComplete(
                        (response, throwable) -> {
                          context.runOnContext(
                              v -> {
                                if (throwable != null) {
                                  handler.handle(io.vertx.core.Future.failedFuture(throwable));
                                } else {
                                  handler.handle(io.vertx.core.Future.succeededFuture(true));
                                }
                              });
                        })));
  }

  @Override
  public Single<List<Map<String, AttributeValue>>> rxQuery(
      String tableName,
      String indexName,
      String keyConditionExpression,
      Map<String, AttributeValue> expressionAttributeValues) {
    Context context = Vertx.currentContext();

    QueryRequest.Builder builder =
        QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression(keyConditionExpression)
            .expressionAttributeValues(expressionAttributeValues);

    if (indexName != null) {
      builder.indexName(indexName);
    }

    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler ->
                dynamoClient
                    .query(builder.build())
                    .whenComplete(
                        (response, throwable) -> {
                          context.runOnContext(
                              v -> {
                                if (throwable != null) {
                                  handler.handle(io.vertx.core.Future.failedFuture(throwable));
                                } else {
                                  handler.handle(
                                      io.vertx.core.Future.succeededFuture(response.items()));
                                }
                              });
                        })));
  }

  @Override
  public Completable rxClose() {
    return withCircuitBreaker(
        Completable.fromAction(
            () -> {
              if (dynamoClient != null) {
                dynamoClient.close();
                log.info("DynamoDB client closed");
              }
            }));
  }
}
