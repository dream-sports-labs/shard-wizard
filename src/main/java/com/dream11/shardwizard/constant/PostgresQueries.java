package com.dream11.shardwizard.constant;

public class PostgresQueries {

  public static final String CREATE_SHARD_QUERY =
      "INSERT INTO ShardMaster (isdefault, details) VALUES ($1, $2) RETURNING shardid;";

  public static final String DEACTIVATE_SHARD_QUERY =
      "UPDATE ShardMaster "
          + "SET isactive = false "
          + "WHERE shardid = $1 "
          + "  AND NOT EXISTS ( "
          + "    SELECT 1 "
          + "    FROM EntityShardMapping "
          + "    WHERE shardids::jsonb @> to_jsonb(ShardMaster.shardid) "
          + "  ); ";
  public static final String GET_ALL_SHARDS_QUERY =
      "SELECT * FROM ShardMaster WHERE isactive = $1;";

  public static final String SET_DEFAULT_FLAG_QUERY_PREFIX =
      "UPDATE ShardMaster SET isdefault = $1 WHERE shardid IN (";

  public static final String GET_DEFAULT_SHARDS_QUERY =
      "SELECT * FROM ShardMaster WHERE isdefault = true and isactive = true;";

  public static final String CREATE_ENTITY_SHARD_MAPPING_QUERY =
      "INSERT INTO EntityShardMapping (entityid, shardids) VALUES ($1, $2);";

  public static final String GET_SHARD_DETAILS_FOR_ENTITY_ID_QUERY =
      "SELECT sm.shardid, sm.details "
          + "FROM ShardMaster sm "
          + "JOIN EntityShardMapping esm "
          + "ON esm.shardids::jsonb @> to_jsonb(sm.shardid) "
          + "WHERE esm.entityid = $1; ";

  public static final String GET_SHARD_DETAILS_FOR_ALL_ENTITIES_QUERY =
      "SELECT sm.shardid, sm.details, esm.entityid "
          + "FROM ShardMaster sm "
          + "JOIN EntityShardMapping esm "
          + "ON esm.shardids::jsonb @> to_jsonb(sm.shardid); ";

  public static final String DELETE_ENTITY_SHARD_MAPPING_QUERY =
      "DELETE FROM EntityShardMapping WHERE entityid = $1";

  public static final String SHOW_TRANSACTION_READ_ONLY = "SHOW transaction_read_only;";

  public static final String SET_SHARD_DETAILS_QUERY =
      "UPDATE ShardMaster SET details = $1 WHERE shardid = $2 RETURNING shardid, details;";

  public static final String GET_SHARD_DETAILS_QUERY =
      "SELECT * FROM ShardMaster WHERE shardid = $1;";
}
