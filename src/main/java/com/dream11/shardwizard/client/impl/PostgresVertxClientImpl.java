package com.dream11.shardwizard.client.impl;

import static com.dream11.shardwizard.constant.Constants.CHECK_READONLY_MODE_INTERVAL_SECONDS;
import static com.dream11.shardwizard.constant.PostgresQueries.SHOW_TRANSACTION_READ_ONLY;

import com.dream11.shardwizard.circuitbreaker.client.AbstractCircuitBreakerClient;
import com.dream11.shardwizard.client.PostgresVertxClient;
import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultSingle;
import io.vertx.reactivex.pgclient.PgPool;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import io.vertx.reactivex.sqlclient.Tuple;
import io.vertx.sqlclient.PoolOptions;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PostgresVertxClientImpl extends AbstractCircuitBreakerClient
    implements PostgresVertxClient {

  private PgPool readerClient;
  private PgPool writerClient;
  private final PgConnectOptions readerPgConnectOptions;
  private final PgConnectOptions writerPgConnectOptions;
  private final PoolOptions poolOptions;
  private final Vertx vertx;
  private ScheduledExecutorService executorService;

  public PostgresVertxClientImpl(
      Vertx vertx,
      PgConnectOptions readerPgConnectOptions,
      PgConnectOptions writerPgConnectOptions,
      PoolOptions poolOptions,
      ShardDetails shardDetails) {
    super(shardDetails);
    this.vertx = vertx;
    this.readerPgConnectOptions = readerPgConnectOptions;
    this.writerPgConnectOptions = writerPgConnectOptions;
    this.poolOptions = poolOptions;
  }

  @Override
  public Completable rxConnect() {
    return withCircuitBreaker(
        vertx
            .rxExecuteBlocking(
                promise -> {
                  try {
                    readerClient = PgPool.pool(vertx, readerPgConnectOptions, poolOptions);
                    writerClient = PgPool.pool(vertx, writerPgConnectOptions, poolOptions);
                    executorService = Executors.newSingleThreadScheduledExecutor();
                    executorService.scheduleAtFixedRate(
                        this::checkIfMasterInReadOnlyMode,
                        CHECK_READONLY_MODE_INTERVAL_SECONDS,
                        CHECK_READONLY_MODE_INTERVAL_SECONDS,
                        TimeUnit.SECONDS);
                    log.info("Successfully Connected to Postgres writer and reader clients");
                    promise.complete();
                  } catch (Exception e) {
                    promise.fail(e);
                  }
                })
            .ignoreElement());
  }

  private void checkIfMasterInReadOnlyMode() {
    try {
      RowSet<Row> rowSet =
          getClient(RdsCluster.WRITER)
              .preparedQuery(SHOW_TRANSACTION_READ_ONLY)
              .rxExecute()
              .blockingGet();
      handleReadOnlyQueryData(rowSet);
    } catch (Exception e) {
      log.error("Error in getting readonly information from master", e);
    }
  }

  private void handleReadOnlyQueryData(RowSet<Row> rowSet) {
    if (rowSet.iterator().hasNext()) {
      String readOnly = rowSet.iterator().next().getString("transaction_read_only");
      log.debug("Readonly mode on masterHost:{}", readOnly);
      if ("on".equals(readOnly)) {
        log.error("current masterHost is not a master, reconnecting");
        rxClose().andThen(Completable.defer(this::rxConnect)).blockingAwait();
      }
    }
  }

  @Override
  public Single<RowSet<Row>> rxExecutePreparedQuery(RdsCluster cluster, String query, Tuple tuple) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              getClient(cluster)
                  .preparedQuery(query)
                  .execute(tuple, result -> context.runOnContext(x -> handler.handle(result)));
            }));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteBatchPreparedQuery(
      RdsCluster cluster, String query, List<Tuple> tuplesBatch) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              getClient(cluster)
                  .preparedQuery(query)
                  .executeBatch(
                      tuplesBatch, result -> context.runOnContext(x -> handler.handle(result)));
            }));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteBatchQuery(
      Transaction transaction, String query, List<Tuple> tuple) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            (handler) -> {
              transaction
                  .preparedQuery(query)
                  .executeBatch(
                      tuple,
                      (result) ->
                          context.runOnContext(
                              (x) -> {
                                handler.handle(result);
                              }));
            }));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteQuery(RdsCluster cluster, String query) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              getClient(cluster)
                  .query(query)
                  .execute(result -> context.runOnContext(x -> handler.handle(result)));
            }));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteQuery(Transaction transaction, String query, Tuple tuple) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            (handler) -> {
              transaction
                  .preparedQuery(query)
                  .execute(
                      tuple,
                      (result) ->
                          context.runOnContext(
                              (x) -> {
                                handler.handle(result);
                              }));
            }));
  }

  @Override
  public Single<Transaction> rxBeginTxn() {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.toSingle(
            handler -> {
              writerClient.begin(result -> context.runOnContext(x -> handler.handle(result)));
            }));
  }

  @Override
  public <T> Single<T> rxGetConnectionForTxn(Function<SqlConnection, Single<T>> function) {
    return withCircuitBreaker(
        rxGetContextFixedConnection()
            .flatMap(
                sqlConnection ->
                    Single.just(sqlConnection.begin())
                        .flatMap(
                            tx -> {
                              Single<T> functionResult = function.apply(sqlConnection);
                              return functionResult
                                  .flatMap(
                                      functionOutput ->
                                          tx.rxCommit().andThen(Single.just(functionOutput)))
                                  .onErrorResumeNext(
                                      err -> tx.rxRollback().andThen(Single.error(err)));
                            })
                        .doFinally(sqlConnection::close)));
  }

  private Single<SqlConnection> rxGetContextFixedConnection() {
    Context context = Vertx.currentContext();
    return AsyncResultSingle.toSingle(
        handler -> {
          writerClient.getConnection(result -> context.runOnContext(x -> handler.handle(result)));
        });
  }

  @Override
  public Single<Boolean> commitTransaction(Transaction transaction) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.<Void>toSingle(
                handler -> {
                  transaction.commit(
                      result ->
                          context.runOnContext(
                              x -> {
                                handler.handle(result);
                              }));
                })
            .map(res -> true)
            .doOnError(
                err ->
                    log.error(
                        "Error while committing transaction due to ==> {}", err.getMessage())));
  }

  @Override
  public Completable rxClose() {
    return withCircuitBreaker(
        vertx
            .rxExecuteBlocking(
                promise -> {
                  try {
                    readerClient.close();
                    writerClient.close();
                    promise.complete();
                  } catch (Exception e) {
                    promise.fail(e);
                  }
                })
            .ignoreElement());
  }

  private PgPool getClient(RdsCluster cluster) {
    return cluster == RdsCluster.WRITER ? writerClient : readerClient;
  }
}
