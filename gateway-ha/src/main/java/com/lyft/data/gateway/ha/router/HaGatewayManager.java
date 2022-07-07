package com.lyft.data.gateway.ha.router;

import com.google.common.collect.ImmutableList;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.persistence.dao.GatewayBackend;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HaGatewayManager implements GatewayBackendManager {
  private JdbcConnectionManager connectionManager;

  public HaGatewayManager(JdbcConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  @Override
  public List<ProxyBackendConfiguration> getAllBackends() {
    try {
      connectionManager.open();
      List<GatewayBackend> proxyBackendList = GatewayBackend.findAll();
      return GatewayBackend.upcast(proxyBackendList);
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public List<ProxyBackendConfiguration> getAllActiveBackends() {
    try {
      connectionManager.open();
      List<GatewayBackend> proxyBackendList = GatewayBackend.where("active = ?", true);
      return GatewayBackend.upcast(proxyBackendList);
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public List<ProxyBackendConfiguration> getActiveAdhocBackends() {
    try {
      connectionManager.open();
      List<GatewayBackend> proxyBackendList =
          GatewayBackend.where("active = ? and routing_group = ?", true, "adhoc");
      return GatewayBackend.upcast(proxyBackendList);
    } catch (Exception e) {
      log.info("Error fetching all backends", e.getLocalizedMessage());
    } finally {
      connectionManager.close();
    }
    return ImmutableList.of();
  }

  @Override
  public List<ProxyBackendConfiguration> getActiveBackends(String routingGroup) {
    try {
      connectionManager.open();
      List<GatewayBackend> proxyBackendList =
          GatewayBackend.where("active = ? and routing_group = ?", true, routingGroup);
      return GatewayBackend.upcast(proxyBackendList);
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public List<RoutingGroupConfiguration> getAllRoutingGroups(
                                         List<ProxyBackendConfiguration> backends) {
    HashMap<String, RoutingGroupConfiguration> mapOfGroups = new HashMap<>();

    backends.forEach(backend -> {
      String routingGroup = backend.getRoutingGroup();
      mapOfGroups.putIfAbsent(routingGroup, new RoutingGroupConfiguration(routingGroup));
      mapOfGroups.get(routingGroup).registerBackend(backend);
    });

    return new ArrayList<RoutingGroupConfiguration>(mapOfGroups.values());
  }

  @Override
  public ProxyBackendConfiguration addBackend(ProxyBackendConfiguration backend) {
    try {
      connectionManager.open();
      GatewayBackend.create(new GatewayBackend(), backend);
    } finally {
      connectionManager.close();
    }
    return backend;
  }

  @Override
  public ProxyBackendConfiguration updateBackend(ProxyBackendConfiguration backend) {
    try {
      connectionManager.open();
      GatewayBackend model = GatewayBackend.findFirst("name = ?", backend.getName());
      if (model == null) {
        GatewayBackend.create(model, backend);
      } else {
        GatewayBackend.update(model, backend);
      }
    } finally {
      connectionManager.close();
    }
    return backend;
  }

  @Override
  public void deleteBackend(String name) {
    try {
      connectionManager.open();
      GatewayBackend.delete("name = ?", name);
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public void deactivateBackend(String backendName) {
    try {
      connectionManager.open();
      GatewayBackend.findFirst("name = ?", backendName).set("active", false).saveIt();
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public void activateBackend(String backendName) {
    try {
      connectionManager.open();
      GatewayBackend.findFirst("name = ?", backendName).set("active", true).saveIt();
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public void pauseRoutingGroup(String routingGroup) {
    try {
      connectionManager.open();
      GatewayBackend.find("routing_group = ?", routingGroup)
                    .forEach(model -> model.set("active", false).saveIt());
    } finally {
      connectionManager.close();
    }
  }

  @Override
  public void resumeRoutingGroup(String routingGroup) {
    try {
      connectionManager.open();
      GatewayBackend.find("routing_group = ?", routingGroup)
                    .forEach(model -> model.set("active", true).saveIt());
    } finally {
      connectionManager.close();
    }
  }
}
