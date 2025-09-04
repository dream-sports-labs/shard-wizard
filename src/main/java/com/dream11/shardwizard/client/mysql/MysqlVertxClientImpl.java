package com.dream11.shardwizard.client.mysql;

import static com.dream11.shardwizard.constant.Constants.CHECK_READONLY_MODE_INTERVAL_SECONDS;
import static com.dream11.shardwizard.constant.Constants.Metric.*;
import static com.dream11.shardwizard.shardmanager.impl.mysql.MysqlQueries.SHOW_VARIABLES_READ_ONLY;

import com.dream11.shardwizard.circuitbreaker.client.AbstractCircuitBreakerClient;
import com.dream11.shardwizard.constant.RdsCluster;
import com.dream11.shardwizard.metric.event.DatabaseEventRecorder;
import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.reactivex.core.Context;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.impl.AsyncResultSingle;
import io.vertx.reactivex.mysqlclient.MySQLPool;
import io.vertx.reactivex.sqlclient.Pool;
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
public class MysqlVertxClientImpl extends AbstractCircuitBreakerClient implements MysqlVertxClient {

  private MySQLPool masterPool;
  private MySQLPool slavePool;
  private final Vertx vertx;
  private final MySQLConnectOptions masterOptions;
  private final MySQLConnectOptions slaveOptions;
  private final PoolOptions poolOptions;
  private long timerId;
  private final DatabaseEventRecorder eventRecorder = DatabaseEventRecorder.getInstance();

  private ScheduledExecutorService executorService;

  public MysqlVertxClientImpl(
      Vertx vertx,
      MySQLConnectOptions masterOptions,
      MySQLConnectOptions slaveOptions,
      PoolOptions poolOptions,
      ShardDetails shardDetails) {
    super(shardDetails);
    this.vertx = vertx;
    this.masterOptions = masterOptions;
    this.slaveOptions = slaveOptions;
    this.poolOptions = poolOptions;
  }

  @Override
  public Completable rxConnect() {
    return withCircuitBreaker(
        vertx
            .rxExecuteBlocking(
                promise -> {
                  createMasterSlavePool();
                  if (masterPool != null) {
                    executorService = Executors.newSingleThreadScheduledExecutor();
                    executorService.scheduleAtFixedRate(
                        this::checkIfMasterInReadOnlyMode,
                        CHECK_READONLY_MODE_INTERVAL_SECONDS,
                        CHECK_READONLY_MODE_INTERVAL_SECONDS,
                        TimeUnit.SECONDS);
                  }
                  log.info("Successfully Connected to MySQL writer and reader clients");
                  eventRecorder.recordSuccess(DB_CONNECT);
                  promise.complete();
                })
            .ignoreElement());
  }

  @Override
  public Single<RowSet<Row>> rxExecutePreparedQuery(RdsCluster cluster, String query, Tuple tuple) {
    return withCircuitBreaker(getPool(cluster).preparedQuery(query).rxExecute(tuple))
        .doOnSuccess(rows -> eventRecorder.recordSuccess(DB_QUERY, cluster, query))
        .doOnError(error -> eventRecorder.recordError(DB_QUERY, cluster, query, error));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteBatchPreparedQuery(
      RdsCluster cluster, String query, List<Tuple> tuplesBatch) {
    return withCircuitBreaker(getPool(cluster).preparedQuery(query).rxExecuteBatch(tuplesBatch))
        .doOnSuccess(rows -> eventRecorder.recordSuccess(DB_BATCH_QUERY, cluster, query))
        .doOnError(error -> eventRecorder.recordError(DB_BATCH_QUERY, cluster, query, error));
  }

  @Override
  public Single<RowSet<Row>> rxExecuteQuery(RdsCluster cluster, String query) {
    return withCircuitBreaker(getPool(cluster).query(query).rxExecute())
        .doOnSuccess(rows -> eventRecorder.recordSuccess(DB_RAW_QUERY, cluster, query))
        .doOnError(error -> eventRecorder.recordError(DB_RAW_QUERY, cluster, query, error));
  }

  @Override
  public Single<Transaction> rxBeginTxn() {
    return withCircuitBreaker(
        masterPool
            .rxGetConnection()
            .flatMap(conn -> Single.fromCallable(conn::begin).cast(Transaction.class)));
  }

  @Override
  public Single<Boolean> rxCommitTransaction(Transaction transaction) {
    Context context = Vertx.currentContext();
    return withCircuitBreaker(
        AsyncResultSingle.<Void>toSingle(
                handler -> {
                  transaction.commit(
                      result -> {
                        context.runOnContext(
                            x -> {
                              handler.handle(result);
                            });
                      });
                })
            .map(res -> true));
  }

  @Override
  public <T> Single<T> rxGetConnectionForTxn(Function<SqlConnection, Single<T>> function) {
    return withCircuitBreaker(
        masterPool
            .rxGetConnection()
            .flatMap(
                conn ->
                    Single.fromCallable(conn::begin)
                        .cast(Transaction.class)
                        .flatMap(
                            tx -> {
                              Single<T> functionResult = function.apply(conn);
                              return functionResult
                                  .flatMap(
                                      result ->
                                          Single.fromCallable(
                                              () -> {
                                                tx.commit();
                                                return result;
                                              }))
                                  .onErrorResumeNext(
                                      err ->
                                          Single.fromCallable(
                                                  () -> {
                                                    tx.rollback();
                                                    return Single.<T>error(err);
                                                  })
                                              .flatMap(x -> x));
                            })
                        .doFinally(conn::close)));
  }

  @Override
  public Completable rxClose() {
    return withCircuitBreaker(
        vertx
            .rxExecuteBlocking(
                promise -> {
                  try {
                    if (timerId != 0) {
                      vertx.cancelTimer(timerId);
                      timerId = 0;
                    }
                    if (masterPool != null) {
                      masterPool.close();
                    }
                    if (slavePool != null) {
                      slavePool.close();
                    }
                    promise.complete();
                  } catch (Exception e) {
                    eventRecorder.recordError(DB_CLOSE, e);
                    promise.fail(e);
                  }
                })
            .ignoreElement());
  }

  private Pool getPool(RdsCluster cluster) {
    return cluster == RdsCluster.WRITER ? masterPool : slavePool;
  }

  private void checkIfMasterInReadOnlyMode() {
    try {
      RowSet<Row> rowSet =
          getPool(RdsCluster.WRITER)
              .preparedQuery(SHOW_VARIABLES_READ_ONLY)
              .rxExecute()
              .blockingGet();
      handleReadOnlyQueryData(rowSet);
    } catch (Exception e) {
      log.error("Error in getting readonly information from master", e);
    }
  }

  private void handleReadOnlyQueryData(RowSet<Row> rowSet) {
    if (rowSet.iterator().hasNext()) {
      String readOnly = rowSet.iterator().next().getString("Value");
      log.info("Readonly mode on masterHost:{}", readOnly);
      if ("ON".equals(readOnly)) {
        log.error("current masterHost is not a master, reconnecting");
        createMasterSlavePool();
      }
    }
  }

  private void createMasterSlavePool() {
    try {
      masterPool = MySQLPool.pool(vertx, masterOptions, poolOptions);
      slavePool = MySQLPool.pool(vertx, slaveOptions, poolOptions);
      log.info("Created mysql master and slave pool");
      log.info("POOL CREATED: master --> {} slave --> {}", masterPool, slavePool);
    } catch (Exception e) {
      log.error("Error in createMasterSlavePool {}", e.getMessage());
    }
  }
}
