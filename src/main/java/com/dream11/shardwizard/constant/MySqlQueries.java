package com.dream11.shardwizard.constant;

public class MySqlQueries {

  public static final String CREATE_SHARD_QUERY =
      "INSERT INTO ShardMaster (IsDefault, Details) VALUES (?, ?)";

  public static final String DEACTIVATE_SHARD_QUERY =
      "UPDATE ShardMaster set IsActive=false "
          + "WHERE ShardId = ? "
          + "AND NOT EXISTS ( "
          + "    SELECT 1 "
          + "    FROM EntityShardMapping "
          + "    WHERE JSON_CONTAINS(EntityShardMapping.ShardIds, CAST(ShardMaster.ShardId AS JSON), '$') "
          + ");";

  public static final String GET_ALL_SHARDS_QUERY = "SELECT * FROM ShardMaster where IsActive=?";

  public static final String SET_DEFAULT_FLAG_QUERY_PREFIX =
      "UPDATE ShardMaster set IsDefault=? where ShardId IN (";

  public static final String GET_DEFAULT_SHARDS_QUERY =
      "SELECT * FROM ShardMaster where IsDefault=true";

  public static final String CREATE_ENTITY_SHARD_MAPPING_QUERY =
      "INSERT INTO EntityShardMapping (EntityId, ShardIds) VALUES (?, ?)";

  public static final String GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY =
      "SELECT sm.ShardId, sm.Details "
          + "FROM ShardMaster sm "
          + "JOIN EntityShardMapping esm "
          + "ON JSON_CONTAINS(esm.ShardIds, CAST(sm.ShardId AS JSON), '$') "
          + "WHERE esm.EntityId = ?;";
  public static final String GET_SHARD_DETAILS_FOR_ALL_ENTITIES_QUERY =
      "SELECT sm.ShardId, sm.Details, esm.EntityId "
          + "FROM ShardMaster sm "
          + "JOIN EntityShardMapping esm "
          + "ON JSON_CONTAINS(esm.ShardIds, CAST(sm.ShardId AS JSON), '$')";

  public static final String DELETE_ENTITY_SHARD_MAPPING_QUERY =
      "DELETE FROM EntityShardMapping WHERE EntityId = ?";

  public static final String SET_SHARD_DETAILS_QUERY =
      "UPDATE ShardMaster SET Details = ? WHERE ShardId = ?";

  public static final String GET_SHARD_DETAILS_QUERY =
      "SELECT ShardId, Details FROM ShardMaster WHERE ShardId = ?";

  public static final String SHOW_VARIABLES_READ_ONLY =
      "show variables where variable_name='innodb_read_only'";
}
