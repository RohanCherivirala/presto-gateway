package com.lyft.data.gateway.ha.config;

import com.fasterxml.jackson.annotation.JsonProperty;

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
  private int numberOfClusters;
  private int activeClusters;

  /**
   * Constructor specifying name and active state of routing group.
   * @param name Name of routing group
   * @param active Activate state of routing group
   */
  public RoutingGroupConfiguration(@JsonProperty("name") String name, 
      @JsonProperty("active") boolean active) {
    this.name = name;
    this.active = active;
    numberOfClusters = 0;
    activeClusters = 0;
  }

  /**
   * Constructor specifying name of routing group.
   * @param name Name of routing group
   */
  public RoutingGroupConfiguration(String name) {
    this(name, true);
  }

  /**
   * Registers a backend associated with the routing group.
   * @param backend Backend to register
   */
  public void registerBackend(ProxyBackendConfiguration backend) {
    if (backend.isActive()) {
      activeClusters++;
    }

    numberOfClusters++;
  }
}
