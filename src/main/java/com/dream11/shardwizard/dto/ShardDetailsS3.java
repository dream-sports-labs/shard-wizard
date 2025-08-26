package com.dream11.shardwizard.dto;

import lombok.*;

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
