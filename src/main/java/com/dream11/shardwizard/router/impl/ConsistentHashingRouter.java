package com.dream11.shardwizard.router.impl;

import com.dream11.shardwizard.router.ShardRouter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.testcontainers.shaded.com.google.common.hash.Hashing;

public class ConsistentHashingRouter implements ShardRouter {

  private static final int DEFAULT_REPLICAS = 2000;
  private static final String SERVER_ID_DELIMITER = "#";

  private final TreeMap<Long, String> ring;
  private final int numberOfReplicas;

  public ConsistentHashingRouter() {
    this.ring = new TreeMap<>();
    this.numberOfReplicas = DEFAULT_REPLICAS;
  }

  @Override
  public void initialize(List<Long> shardIds) {
    for (long shardId : shardIds) {
      addServer(shardId + SERVER_ID_DELIMITER);
    }
  }

  @Override
  public long getRoutedShardId(String routeKey) {
    String serverId = getServer(routeKey);
    return Long.parseLong(serverId.split(SERVER_ID_DELIMITER)[0]);
  }

  public void addServer(String server) {
    for (int i = 0; i < numberOfReplicas; i++) {
      long hash = generateHash(server + i);
      ring.put(hash, server);
    }
  }

  public void removeServer(String server) {
    for (int i = 0; i < numberOfReplicas; i++) {
      long hash = generateHash(server + i);
      ring.remove(hash);
    }
  }

  public String getServer(String key) {
    if (ring.isEmpty()) {
      return null;
    }
    long hash = generateHash(key);
    if (!ring.containsKey(hash)) {
      SortedMap<Long, String> tailMap = ring.tailMap(hash);
      hash = tailMap.isEmpty() ? ring.firstKey() : tailMap.firstKey();
    }
    return ring.get(hash);
  }

  private long generateHash(String key) {
    return Hashing.murmur3_128().hashString(key, StandardCharsets.UTF_8).asLong();
  }
}
