package com.dream11.shardwizard.client.impl.postgres.vertx;

import com.dream11.shardwizard.client.impl.common.RdsCluster;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.sqlclient.Row;
import io.vertx.reactivex.sqlclient.RowSet;
import io.vertx.reactivex.sqlclient.SqlConnection;
import io.vertx.reactivex.sqlclient.Transaction;
import io.vertx.reactivex.sqlclient.Tuple;
import java.util.List;
import java.util.function.Function;

/**
 * This interface defines methods for interacting with a Postgres database using the reactive Vert.x
 * client. It is designed to ensure that each client callbacks remains tied to the same event-loop
 * context, preventing the context-switching issues that arise when a single global client is used
 * across the application.
 */
public interface PostgresVertxClient {

  Completable rxConnect();

  Single<RowSet<Row>> rxExecutePreparedQuery(RdsCluster cluster, String query, Tuple tuple);

  Single<RowSet<Row>> rxExecuteBatchPreparedQuery(
      RdsCluster cluster, String query, List<Tuple> tuplesBatch);

  Single<RowSet<Row>> rxExecuteBatchQuery(Transaction transaction, String query, List<Tuple> tuple);

  Single<RowSet<Row>> rxExecuteQuery(RdsCluster cluster, String query);

  Single<RowSet<Row>> rxExecuteQuery(Transaction transaction, String query, Tuple tuple);

  Single<Transaction> rxBeginTxn();

  <T> Single<T> rxGetConnectionForTxn(Function<SqlConnection, Single<T>> function);

  Single<Boolean> commitTransaction(Transaction transaction);

  Completable rxClose();
}
