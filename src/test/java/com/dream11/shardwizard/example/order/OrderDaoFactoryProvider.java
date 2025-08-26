package com.dream11.shardwizard.example.order;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderDaoFactoryProvider implements Provider<OrderDaoFactory> {

  @Inject Vertx vertx;

  @Override
  public OrderDaoFactory get() {
    log.info("Creating OrderDaoFactory");
    OrderDaoFactory orderDaoFactory = new OrderDaoFactory(vertx);
    // Remove blocking operation and let the caller handle the bootstrap
    return orderDaoFactory;
  }
}
