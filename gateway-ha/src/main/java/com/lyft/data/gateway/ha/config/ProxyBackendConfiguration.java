package com.lyft.data.gateway.ha.config;

import com.lyft.data.server.config.GatewayServerConfiguration;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = true)
public class ProxyBackendConfiguration extends GatewayServerConfiguration {
  private boolean active = true;
  private String routingGroup = "adhoc";
}
