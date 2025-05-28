package com.dream11.shardwizard.example.order;

import com.google.inject.Inject;
import com.google.inject.Provider;
import io.vertx.reactivex.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class OrderDaoFactoryProvider implements Provider<OrderAbstractDaoFactory> {

  @Inject
  Vertx vertx;

  @Override
  public OrderAbstractDaoFactory get() {
    log.info("Creating OrderAbstractDaoFactory");
    OrderAbstractDaoFactory orderDaoFactory = new OrderAbstractDaoFactory(vertx);
    // Remove blocking operation and let the caller handle the bootstrap
    return orderDaoFactory;
  }
}
