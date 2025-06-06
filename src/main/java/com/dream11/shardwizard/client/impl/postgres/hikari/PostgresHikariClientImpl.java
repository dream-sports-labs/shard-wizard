package com.dream11.shardwizard.client.impl.postgres.hikari;

import static com.dream11.shardwizard.constant.Constants.CHECK_READONLY_MODE_INTERVAL_SECONDS;
import static com.dream11.shardwizard.shardmanager.impl.postgres.PostgresQueries.SHOW_TRANSACTION_READ_ONLY;

import com.dream11.shardwizard.client.impl.common.RdsCluster;
import com.dream11.shardwizard.utils.ExceptionalFunction;
import com.zaxxer.hikari.HikariDataSource;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultCompletable;
import io.vertx.reactivex.impl.AsyncResultSingle;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetFactory;
import javax.sql.rowset.RowSetProvider;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresHikariClientImpl implements PostgresHikariClient {

  private final PostgresHikariConfig postgresHikariConfig;
  private final Vertx vertx;

  private HikariDataSource masterDataSource;
  private HikariDataSource slaveDataSource;
  private ScheduledExecutorService executorService;

  public PostgresHikariClientImpl(Vertx vertx, PostgresHikariConfig postgresHikariConfig) {
    this.vertx = vertx;
    this.postgresHikariConfig = postgresHikariConfig;
  }

  private void connect(Handler<AsyncResult<Void>> handler) {
    vertx.executeBlocking(
        promise -> {
          try {
            this.masterDataSource =
                new HikariDataSource(postgresHikariConfig.getClientConfig(true));
            this.slaveDataSource =
                new HikariDataSource(postgresHikariConfig.getClientConfig(false));
            executorService = Executors.newSingleThreadScheduledExecutor();
            promise.complete();
          } catch (Exception e) {
            promise.fail(e);
          }
        },
        asyncResult -> {
          if (asyncResult.succeeded()) {
            executorService.scheduleAtFixedRate(
                this::checkIfMasterInReadOnlyMode,
                CHECK_READONLY_MODE_INTERVAL_SECONDS,
                CHECK_READONLY_MODE_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
            log.info("Successfully created master and slave");
            handler.handle(Future.succeededFuture());
          } else {
            handler.handle(Future.failedFuture(asyncResult.cause()));
          }
        });
  }

  private void checkIfMasterInReadOnlyMode() {
    String readOnlyQueryValue = getReadOnlyInformation();
    log.debug("Readonly mode on masterHost:{}", readOnlyQueryValue);
    if ("on".equals(readOnlyQueryValue)) {
      log.error("current masterHost is not a master, reconnecting");
      try {
        rxClose()
            .andThen(Completable.defer(this::rxConnect))
            .blockingAwait(CHECK_READONLY_MODE_INTERVAL_SECONDS, TimeUnit.SECONDS);
      } catch (Exception e) {
        log.error("Error in reconnecting", e);
      }
    }
  }

  private String getReadOnlyInformation() {
    try (Connection connection = getConnection(RdsCluster.WRITER);
        PreparedStatement preparedStatement =
            connection.prepareStatement(SHOW_TRANSACTION_READ_ONLY);
        CachedRowSet cachedRowSet = RowSetProvider.newFactory().createCachedRowSet(); ) {
      cachedRowSet.populate(preparedStatement.executeQuery());
      cachedRowSet.next();
      return cachedRowSet.getString("transaction_read_only");
    } catch (Exception e) {
      log.error("Error in getting readonly information from master", e);
      return null;
    }
  }

  @Override
  public Completable rxConnect() {
    return AsyncResultCompletable.toCompletable(this::connect);
  }

  @SneakyThrows
  @Override
  public Connection getConnection(RdsCluster cluster) {
    return cluster == RdsCluster.WRITER
        ? this.masterDataSource.getConnection()
        : this.slaveDataSource.getConnection();
  }

  @Override
  public Single<ResultSet> rxReadDBCall(RdsCluster cluster, String query, List<Object> params) {
    return AsyncResultSingle.toSingle(handler -> readDBCall(query, params, cluster, handler));
  }

  private void readDBCall(
      String query,
      List<Object> params,
      RdsCluster cluster,
      Handler<AsyncResult<ResultSet>> handler) {
    Vertx.currentContext()
        .executeBlocking(
            promise -> {
              try (Connection connection = getConnection(cluster);
                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                RowSetFactory factory = RowSetProvider.newFactory();
                CachedRowSet cachedRowSet = factory.createCachedRowSet();
                if (Objects.nonNull(params)) {
                  for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                  }
                }
                cachedRowSet.populate(preparedStatement.executeQuery());
                promise.complete(cachedRowSet);
              } catch (Exception e) {
                promise.fail(e);
              }
            },
            asyncResult -> {
              if (asyncResult.succeeded()) {
                handler.handle(Future.succeededFuture((ResultSet) asyncResult.result()));
              } else {
                handler.handle(Future.failedFuture(asyncResult.cause()));
              }
            });
  }

  @Override
  public Single<Integer> rxModifyDBCall(String query, List<Object> params) {
    return AsyncResultSingle.toSingle(handler -> modifyDBCall(query, params, handler));
  }

  private void modifyDBCall(
      String query, List<Object> params, Handler<AsyncResult<Integer>> handler) {
    Vertx.currentContext()
        .executeBlocking(
            promise -> {
              try (Connection connection = getConnection(RdsCluster.WRITER);
                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                if (Objects.nonNull(params)) {
                  for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                  }
                }
                promise.complete(preparedStatement.executeUpdate());
              } catch (Exception e) {
                promise.fail(e);
              }
            },
            false,
            asyncResult -> {
              if (asyncResult.succeeded()) {
                handler.handle(Future.succeededFuture((Integer) asyncResult.result()));
              } else {
                handler.handle(Future.failedFuture(asyncResult.cause()));
              }
            });
  }

  @Override
  public Single<Integer> rxModifyBatchDBCall(String query, List<List<Object>> paramsBatch) {
    return AsyncResultSingle.toSingle(
        handler -> batchDBCall(query, paramsBatch, RdsCluster.WRITER, handler));
  }

  private void batchDBCall(
      String query,
      List<List<Object>> batch,
      RdsCluster cluster,
      Handler<AsyncResult<Integer>> handler) {
    Vertx.currentContext()
        .executeBlocking(
            promise -> {
              try (Connection connection = getConnection(cluster);
                  PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                for (List<Object> params : batch) {
                  for (int i = 0; i < params.size(); i++) {
                    preparedStatement.setObject(i + 1, params.get(i));
                  }
                  preparedStatement.addBatch();
                }
                promise.complete(preparedStatement.executeBatch());
              } catch (Exception e) {
                promise.fail(e);
              }
            },
            false,
            asyncResult -> {
              if (asyncResult.succeeded()) {
                handler.handle(Future.succeededFuture(1));
              } else {
                handler.handle(Future.failedFuture(asyncResult.cause()));
              }
            });
  }

  @Override
  public Completable rxClose() {
    return AsyncResultCompletable.toCompletable(t -> close());
  }

  @Override
  public <T> Single<T> rxGetConnectionForTxn(ExceptionalFunction<Connection, T> codeFunction) {
    return Vertx.currentContext()
        .<T>rxExecuteBlocking(
            promise -> {
              try (Connection connection = getConnection(RdsCluster.WRITER)) {
                T result = executeFunctionAndGetResult(codeFunction, connection);
                promise.complete(result);
              } catch (Exception e) {
                promise.fail(e);
              }
            },
            false)
        .toSingle();
  }

  private <T> T executeFunctionAndGetResult(
      ExceptionalFunction<Connection, T> blockingCodeFunction, Connection connection)
      throws Exception {
    try {
      connection.setAutoCommit(false);
      T result = blockingCodeFunction.apply(connection);
      connection.commit();
      return result;
    } catch (Exception e) {
      connection.rollback();
      throw e;
    }
  }

  private void close() {
    if (Objects.nonNull(masterDataSource)) {
      masterDataSource.close();
    }
    if (Objects.nonNull(slaveDataSource)) {
      slaveDataSource.close();
    }
  }
}
