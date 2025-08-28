package com.dream11.shardwizard.dao;

import com.dream11.shardwizard.model.ShardDetails;
import io.reactivex.Completable;
import io.reactivex.Single;
import java.util.List;

public interface DaoFactory<T> {

  /**
   * Gets shard details for the given entity id. There can be multiple shards present for an entity.
   * If mapping is not present for the entity, it will create the mapping and return the shard
   * details. In case mapping is already present but the shard is inActive then will throw
   * ShardNotPresentException.
   *
   * @param entityId EntityId used in getting mapped shards
   * @return List of ShardDetails
   */
  Single<List<ShardDetails>> rxGetOrCreateEntityShardDetails(String entityId);

  /**
   * Gets shard details for the given entity id. There can be multiple shards present for an entity.
   * If mapping is not present for the entity, it will NOT create the mapping and return the shard
   * details. and will throw EntityShardMappingNotPresentException.
   *
   * @param entityId EntityId used in getting mapped shards
   * @return List of ShardDetails
   */
  Single<List<ShardDetails>> rxGetEntityShardDetails(String entityId);

  /**
   * @return List of all active DAO instances.
   */
  Single<List<T>> rxGetAllActiveShardsDAOs();

  /**
   * Gets the connected dao instance for the given entity id. If the dao is not connected, it will
   * connect the dao. If an inActive shard is mapped to the entity, it will throw
   * ShardNotPresentException.
   *
   * @param entityId EntityId used in getting mapped shards
   * @param routeKey key used in routing to a particular shard
   * @return Connected DAO instance
   */
  Single<T> rxGetOrCreateEntityShardDao(String entityId, long routeKey);

  Single<T> rxGetOrCreateEntityShardDao(String entityId, String routeKey);

  /**
   * Gets the connected dao instance by the primary key
   *
   * @param primaryKey
   * @return Connected DAO instance
   */
  Single<T> rxGetDaoInstanceByPrimaryKey(String primaryKey);

  /**
   * Creates connection to each of the active Shard.
   *
   * @return Completable
   */
  Completable rxBootstrap();
}
