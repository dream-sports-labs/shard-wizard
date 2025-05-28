package com.dream11.shardwizard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
@Builder
@NoArgsConstructor
public class ShardDetails {

  private long shardId;
  private ShardConfig shardConfig;
}

