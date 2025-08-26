package com.dream11.shardwizard.example.dto;

import java.util.List;
import lombok.Data;

@Data
public class DBShardConfigDTO {
  private ShardManager shardManager;
  private List<Shard> shards;
  private String userName;
  private String password;
  private String dbName;

  @Data
  public static class ShardManager {
    private int port;
    private String initScript;
  }

  @Data
  public static class Shard {
    private int port;
    private String initScript;
  }
}
