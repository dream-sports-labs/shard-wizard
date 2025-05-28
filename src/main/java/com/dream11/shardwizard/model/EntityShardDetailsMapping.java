package com.dream11.shardwizard.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@AllArgsConstructor
@Builder
public class EntityShardDetailsMapping {

  private String entityId;
  private List<ShardDetails> activeShards;
}
