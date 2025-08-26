package com.dream11.shardwizard.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShardUpdateResponse {
  private ShardDetails currentShardDetails;
  private ShardDetails updatedShardDetails;
}
