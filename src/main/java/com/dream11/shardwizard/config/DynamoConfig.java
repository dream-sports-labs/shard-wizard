package com.dream11.shardwizard.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class DynamoConfig extends SourceConfig {

  @NonNull private String region;
  @NonNull private String accessKey;
  @NonNull private String secretKey;
  private String endpointOverride;
}
