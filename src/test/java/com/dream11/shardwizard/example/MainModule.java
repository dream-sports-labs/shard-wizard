package com.dream11.shardwizard.example;

import com.dream11.shardwizard.example.order.OrderAbstractDaoFactory;
import com.dream11.shardwizard.example.order.OrderDaoFactoryProvider;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.reactivex.core.Vertx;

public class MainModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(OrderAbstractDaoFactory.class)
        .toProvider(OrderDaoFactoryProvider.class)
        .in(Singleton.class);
  }

  @Provides
  @Singleton
  public Vertx provideVertx() {
    return Vertx.vertx(); // Provide a single Vertx instance
  }
}
