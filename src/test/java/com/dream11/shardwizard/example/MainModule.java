package com.dream11.shardwizard.example;

import com.dream11.shardwizard.constant.DatabaseType;
import com.dream11.shardwizard.example.order.OrderDaoFactory;
import com.dream11.shardwizard.example.order.OrderDaoFactoryProvider;
import com.dream11.shardwizard.utils.ShardManagerConfigLoader;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.vertx.reactivex.core.Vertx;

public class MainModule extends AbstractModule {

  private final DatabaseType databaseType;

  public MainModule() {
    this(null); // Use system property approach
  }

  public MainModule(DatabaseType databaseType) {
    this.databaseType = databaseType;
  }

  @Override
  protected void configure() {
    bind(OrderDaoFactory.class).toProvider(OrderDaoFactoryProvider.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  public Vertx provideVertx() {
    return Vertx.vertx(); // Provide a single Vertx instance
  }

  @Provides
  @Singleton
  public DatabaseType provideDatabaseType() {
    // Use centralized database type resolution
    return ShardManagerConfigLoader.resolveDatabaseType(databaseType);
  }
}
