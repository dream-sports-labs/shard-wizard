package com.dream11.shardwizard.model;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class EntityShardMappingS3 {

  private String entityId;
  private List<Long> shardIds;
}
