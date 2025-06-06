package com.dream11.shardwizard.example.order;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.dao.AbstractDaoFactory;
import com.dream11.shardwizard.example.order.impl.CustomDatabaseOrderDaoImpl;
import com.dream11.shardwizard.example.order.impl.PostgresOrderDaoImpl;
import com.dream11.shardwizard.model.ShardDetails;
import io.vertx.reactivex.core.Vertx;
import lombok.NonNull;

public class OrderAbstractDaoFactory extends AbstractDaoFactory<OrderDao> {

  public OrderAbstractDaoFactory(Vertx vertx) {
    super(vertx);
  }

  @Override
  @NonNull
  protected OrderDao getDaoImplFromType(ShardDetails shardDetails) {
    DatabaseType databaseType = shardDetails.getShardConfig().getDatabaseType();
    switch (databaseType) {
      case POSTGRES:
        return new PostgresOrderDaoImpl(vertx, shardDetails);
      case DYNAMODB:
        return new CustomDatabaseOrderDaoImpl(vertx, shardDetails);
      default:
        throw new IllegalArgumentException("Invalid database type");
    }
  }

  @Override
  protected long getShardIdFromPrimaryKey(String primaryKey) {
    if (primaryKey == null || primaryKey.trim().isEmpty()) {
      throw new IllegalArgumentException("Primary key cannot be null or empty");
    }

    String[] parts = primaryKey.trim().split("-");
    if (parts.length != 3) {
      throw new IllegalArgumentException("Invalid primary key format");
    }

    try {
      return Long.parseLong(parts[1]); // Get shard ID from the second part
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "Invalid shard ID format in primary key: " + primaryKey, e);
    }
  }
}
