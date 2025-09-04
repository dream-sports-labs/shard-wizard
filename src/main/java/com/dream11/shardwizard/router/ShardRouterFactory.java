package com.dream11.shardwizard.router;

import com.dream11.shardwizard.constant.RouterType;
import com.dream11.shardwizard.router.impl.ConsistentHashingRouter;
import com.dream11.shardwizard.router.impl.ModuloRouter;
import lombok.experimental.UtilityClass;

/** Factory class for creating ShardRouter instances based on configuration. */
@UtilityClass
public class ShardRouterFactory {

  /**
   * Creates a ShardRouter instance based on the specified router type.
   *
   * @param routerType The type of router to create
   * @return A new ShardRouter instance
   * @throws IllegalArgumentException if the router type is not supported
   */
  public static ShardRouter createRouter(RouterType routerType) {
    if (routerType == null) {
      // Default fallback to MODULO for backward compatibility
      routerType = RouterType.MODULO;
    }

    switch (routerType) {
      case MODULO:
        return new ModuloRouter();
      case CONSISTENT:
        return new ConsistentHashingRouter();
      default:
        throw new IllegalArgumentException("Unsupported router type: " + routerType);
    }
  }
}
