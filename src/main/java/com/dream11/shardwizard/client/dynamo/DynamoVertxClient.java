package com.dream11.shardwizard.client.dynamo;

import io.reactivex.Completable;
import io.reactivex.Single;
import java.util.List;
import java.util.Map;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public interface DynamoVertxClient {

  Completable rxConnect();

  Single<Boolean> rxWriteItem(String tableName, Map<String, AttributeValue> tuple);

  Single<Map<String, AttributeValue>> rxGetItem(
      String tableName, Map<String, AttributeValue> tuple);

  Single<Boolean> rxBatchWriteItem(String tableName, List<Map<String, AttributeValue>> itemsBatch);

  Single<Boolean> rxTransactWriteItems(List<TransactWriteItem> items);

  Single<List<Map<String, AttributeValue>>> rxQuery(
      String tableName,
      String indexName,
      String keyConditionExpression,
      Map<String, AttributeValue> expressionAttributeValues);

  Single<Boolean> rxUpdateItem(
      String tableName, Map<String, AttributeValue> key, Map<String, AttributeValueUpdate> updates);

  Single<Boolean> rxDeleteItem(String tableName, Map<String, AttributeValue> key);

  Completable rxClose();
}
