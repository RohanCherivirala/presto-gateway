package com.lyft.data.gateway.ha.config;

import lombok.Data;
import lombok.ToString;

/**
 * This class stores information about a routing group.
 */
@Data
@ToString
public class RoutingGroupConfiguration {
  private String name;
  private boolean active;
  private int groupSize;
  private int activeClusters;

  public RoutingGroupConfiguration(String name) {
    this.name = name;
    active = false;
    groupSize = 0;
    activeClusters = 0;
  }

  /**
   * Registers a backend associated with the routing group.
   * @param backend Backend to register
   */
  public void registerBackend(ProxyBackendConfiguration backend) {
    if (backend.isActive()) {
      active = true;
      activeClusters++;
    }

    groupSize++;
  }
}
