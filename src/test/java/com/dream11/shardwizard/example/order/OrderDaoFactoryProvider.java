package com.dream11.shardwizard.example.order;

import com.dream11.shardwizard.constant.DatabaseType;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderDaoFactoryProvider implements Provider<OrderDaoFactory> {

  @Inject Vertx vertx;

  // Optional injection - for backward compatibility when DatabaseType is not
  // configured
  @com.google.inject.Inject(optional = true)
  DatabaseType sourceType;

  @Override
  public OrderDaoFactory get() {
    if (sourceType != null) {
      log.info("Creating OrderDaoFactory with explicit sourceType: {}", sourceType);
      return new OrderDaoFactory(vertx, sourceType);
    } else {
      log.info("Creating OrderDaoFactory with fallback sourceType resolution");
      return new OrderDaoFactory(vertx); // Uses fallback mechanism
    }
  }
}
