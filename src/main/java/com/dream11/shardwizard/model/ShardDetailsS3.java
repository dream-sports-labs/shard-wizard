package com.dream11.shardwizard.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ShardDetailsS3 {

  private long shardId;
  private boolean isActive;
  private boolean isDefault;
  private ShardConfig details;
}
