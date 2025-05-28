package com.dream11.shardwizard.constant;

import lombok.ToString;

@ToString
public enum DatabaseType {
  MYSQL,
  POSTGRES,
  SPANNER,
  DYNAMODB,
  REDIS,
  AEROSPIKE
}
