package com.dream11.shardwizard.model;

import com.dream11.shardwizard.constant.DatabaseType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
@Builder
public class ShardConfig {

  @NonNull private DatabaseType databaseType;

  @NonNull private ShardConnectionParameters shardConnectionParams;
}
