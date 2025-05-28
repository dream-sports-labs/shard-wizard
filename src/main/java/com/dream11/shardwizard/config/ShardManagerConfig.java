package com.dream11.shardwizard.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.typesafe.config.Optional;
import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class ShardManagerConfig {

  private final ObjectMapper objectMapper = new ObjectMapper();
  @NonNull
  private String sourceType;
  @Optional
  private int shardsRefreshSeconds = 60;
  private Map<String, Object> sources;

  public SourceConfig convertToSourceConfig(String key) {
    try {
      return objectMapper.convertValue(sources.get(key), SourceConfig.class);
    } catch (Exception e) {
      throw new RuntimeException("Error converting config for key: " + key, e);
    }
  }
}
