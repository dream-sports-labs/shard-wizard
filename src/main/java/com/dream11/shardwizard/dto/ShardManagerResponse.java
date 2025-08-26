package com.dream11.shardwizard.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class ShardManagerResponse {

  private boolean success;
  private String message;
}
