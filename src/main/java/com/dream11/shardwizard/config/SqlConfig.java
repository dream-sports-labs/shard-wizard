package com.dream11.shardwizard.config;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Data
@NoArgsConstructor
public class SqlConfig extends SourceConfig {
  @NonNull private String writerHost;
  @NonNull private String readerHost;
  private int port;
  @NonNull private String database;
  @NonNull private String username;
  @NonNull private String password;
}
