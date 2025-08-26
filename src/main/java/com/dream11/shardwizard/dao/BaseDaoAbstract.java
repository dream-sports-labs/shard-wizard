package com.dream11.shardwizard.dao;

import com.dream11.shardwizard.dto.ShardDetails;
import io.reactivex.Completable;

public abstract class BaseDaoAbstract {

  public abstract Completable rxConnect();

  public abstract Completable rxCloseConnections();

  public abstract ShardDetails getShardDetails();
}
