package com.dream11.shardwizard.client;

import com.dream11.shardwizard.constant.ExceptionalFunction;
import com.dream11.shardwizard.constant.RdsCluster;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

/** This interface provides methods to interact with the Postgres database using Hikari client. */
public interface PostgresHikariClient {

  Completable rxConnect();

  Connection getConnection(RdsCluster cluster);

  Single<ResultSet> rxReadDBCall(RdsCluster cluster, String query, List<Object> params);

  Single<Integer> rxModifyDBCall(String query, List<Object> params);

  Single<Integer> rxModifyBatchDBCall(String query, List<List<Object>> paramsBatch);

  <T> Single<T> rxGetConnectionForTxn(ExceptionalFunction<Connection, T> codeFunction);

  Completable rxClose();
}
