package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;

import java.util.List;

public interface GatewayBackendManager {
  List<ProxyBackendConfiguration> getAllBackends();

  List<ProxyBackendConfiguration> getAllActiveBackends();

  List<ProxyBackendConfiguration> getActiveAdhocBackends();

  List<ProxyBackendConfiguration> getActiveBackends(String routingGroup);

  List<RoutingGroupConfiguration> getAllRoutingGroups(List<ProxyBackendConfiguration> backends);

  ProxyBackendConfiguration addBackend(ProxyBackendConfiguration backend);

  ProxyBackendConfiguration updateBackend(ProxyBackendConfiguration backend);

  void deleteBackend(String backendName);

  void deactivateBackend(String backendName);

  void activateBackend(String backendName);

  void pauseRoutingGroup(String routingGroup);

  void resumeRoutingGroup(String routingGroup);
}
