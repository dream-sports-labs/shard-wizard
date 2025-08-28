package com.dream11.shardwizard.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class S3Config extends SourceConfig {
  @NonNull private String bucketName;
  @NonNull private String shardMasterFilePath;
  @NonNull private String entityShardMappingFolderPath;
  @NonNull private String region;
  @NonNull private String accessKey;
  @NonNull private String secretKey;
  private String endpointOverride;
  private boolean forcePathStyle;
}
