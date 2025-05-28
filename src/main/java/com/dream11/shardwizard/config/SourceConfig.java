package com.dream11.shardwizard.config;

import com.dream11.shardwizard.model.ShardConnectionParameters;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.stream.Collectors;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = S3Config.class, name = "S3"),
  @JsonSubTypes.Type(value = SqlConfig.class, name = "POSTGRES")
})
public abstract class SourceConfig {

  private final ObjectMapper objectMapper = new ObjectMapper();

  @JsonProperty("databaseDefaultShardConfigMap")
  private Map<String, Object> databaseDefaultShardConfigMap;

  public Map<String, ShardConnectionParameters> getParsedShardConfigMap() {
    return databaseDefaultShardConfigMap.entrySet().stream()
        .collect(
            Collectors.toMap(Map.Entry::getKey, entry -> convertToShardConfig(entry.getValue())));
  }

  private ShardConnectionParameters convertToShardConfig(Object obj) {
    if (obj instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> configMap = (Map<String, Object>) obj;
      try {
        return objectMapper.convertValue(configMap, ShardConnectionParameters.class);
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException("Invalid configuration format for shard config.", e);
      }
    }
    throw new IllegalArgumentException("Invalid configuration format for shard config.");
  }
}
