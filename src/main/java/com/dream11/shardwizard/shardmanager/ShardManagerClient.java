package com.dream11.shardwizard.shardmanager;

import com.dream11.shardwizard.config.ShardManagerConfig;
import com.dream11.shardwizard.model.EntityShardDetailsMapping;
import com.dream11.shardwizard.model.ShardConfig;
import com.dream11.shardwizard.model.ShardDetails;
import com.dream11.shardwizard.model.ShardManagerResponse;
import com.dream11.shardwizard.model.ShardUpdateResponse;
import com.dream11.shardwizard.shardmanager.impl.ShardManagerFactory;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import java.util.List;

public interface ShardManagerClient {

  /**
   * This method is used to create a ShardManagerClient instance. It reads the configuration from
   * the config folder, creates and returns a ShardManagerClientImpl instance. By default, the
   * postgres implementation of the client is used.
   *
   * @param vertx Vertx instance
   * @return ShardManagerClient instance
   */
  static ShardManagerClient create(Vertx vertx) {
    return ShardManagerFactory.getShardManagerClient(vertx);
  }

  static ShardManagerClient create(Vertx vertx, ShardManagerConfig shardManagerConfig) {
    return ShardManagerFactory.getShardManagerClient(vertx, shardManagerConfig);
  }

  Completable rxBootstrap();

  /**
   * This method closes the connections of the source client which is used by the ShardManagerClient
   * instance.
   *
   * @return Completable
   */
  Completable rxClose();

  /**
   * This method is used to register a new shard. It creates a new shard in the source and returns
   * the details of the shard.
   *
   * @param isDefault true if the shard is default
   * @param details ShardConfig object containing the details of the shard
   * @return Single containing the ShardDetails object
   */
  Single<ShardDetails> rxRegisterNewShard(boolean isDefault, ShardConfig details);

  /**
   * This method is used to set the default flag of the shard. Default shards are the ones which are
   * supplied in automatic creation of entity--to-->shardIds mapping.
   *
   * @param shardIds List of shardIds
   * @param isDefault true if the shard is default
   * @return Single containing the ShardManagerResponse object
   */
  Single<ShardManagerResponse> rxSetDefaultFlag(List<Long> shardIds, boolean isDefault);

  /**
   * This method is used to deactivate a shard. Deactivated shards cannot be used for future
   * operations.
   *
   * @param shardId long number representing the shardId to be deactivated
   * @return Single containing the ShardManagerResponse object
   */
  Single<ShardManagerResponse> rxDeactivateShard(long shardId);

  /**
   * This method is used to get the list of active shards.
   *
   * @return Single containing the list of active shards
   */
  Single<List<ShardDetails>> rxGetActiveShards();

  /**
   * This method is used to find the mapping of an entity to shards. If the mapping is not found, a
   * mapping is created with default shardIds and returned.
   *
   * @param entityId EntityId for which mapping is to be found
   * @return Single containing the EntityShardMapping object
   */
  Single<EntityShardDetailsMapping> rxFindMappingOrCreateDefault(String entityId);

  Single<EntityShardDetailsMapping> rxFindMapping(String entityId);

  /**
   * This method is used to get the all the EntityShardMappings stored in the shard manager source.
   *
   * @return Single containing the list of EntityShardMapping
   */
  Single<List<EntityShardDetailsMapping>> rxListEntityShardMappings();

  /**
   * This method is used to delete an EntityShardMapping corresponding to the entityId.
   *
   * @param entityId EntityId for which mapping is to be deleted
   * @return Single containing the ShardManagerResponse object
   */
  Single<ShardManagerResponse> rxDeleteEntityShardMapping(String entityId);

  /**
   * This method is used to create a mapping of entityId to the shardIds.
   *
   * @param entityId EntityId for which mapping is to be established
   * @param shardIds List of shardIds to be mapped to the entityId
   * @return Single containing the ShardManagerResponse object
   */
  Single<ShardManagerResponse> rxEstablishEntityToShardsMapping(
      String entityId, List<Long> shardIds);

  Single<ShardUpdateResponse> rxUpdateExistingShardDetails(long shardId, ShardConfig details);
}
