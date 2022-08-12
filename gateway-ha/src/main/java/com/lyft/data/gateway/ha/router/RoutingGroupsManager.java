package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.persistence.dao.RoutingGroups;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;

/**
 * This class contians functions to manage routing groups and get information
 * about them from the table and ensure that data is synchronized between
 * all the tables.
 */
@Slf4j
public class RoutingGroupsManager {
  private JdbcConnectionManager connectionManager;

  public RoutingGroupsManager(JdbcConnectionManager connectionManager) {
    this.connectionManager = connectionManager;
  }

  /**
   * Returns a list of routing groups based on the active backends
   * and combines information with the backends.
   * @param backends List of backends
   * @return List of routing groups
   */
  public List<RoutingGroupConfiguration> getAllRoutingGroups(
                                         List<ProxyBackendConfiguration> backends) {
    List<RoutingGroupConfiguration> groupsFromBackends = getAllRoutingGroupsFromBackends(backends);
    List<RoutingGroupConfiguration> groupsFromDatabase = getAllRountingGroupsFromTable();

    for (RoutingGroupConfiguration group : groupsFromDatabase) {
      RoutingGroupConfiguration groupFromBackends = getByName(groupsFromBackends, group.getName());

      if (groupFromBackends != null) {
        group.setActiveClusters(groupFromBackends.getActiveClusters());
        group.setNumberOfClusters(groupFromBackends.getNumberOfClusters());
      }
    }

    return groupsFromDatabase;
  }

  /**
   * Returns a list of routing groups contianing information from
   * the database.
   * @return List of routing groups from database
   */
  private List<RoutingGroupConfiguration> getAllRountingGroupsFromTable() {
    try {
      connectionManager.open();
      List<RoutingGroups> routingGroupsList = RoutingGroups.findAll();
      return RoutingGroups.upcast(routingGroupsList);
    } finally {
      connectionManager.close();
    }
  }

  /**
   * Returns a list of routing groups based on the active backends.
   * @param backends List of backends
   * @return List of routing groups
   */
  private List<RoutingGroupConfiguration> getAllRoutingGroupsFromBackends(
                                         List<ProxyBackendConfiguration> backends) {
    HashMap<String, RoutingGroupConfiguration> mapOfGroups = new HashMap<>();

    backends.forEach(backend -> {
      String routingGroup = backend.getRoutingGroup();
      mapOfGroups.putIfAbsent(routingGroup, new RoutingGroupConfiguration(routingGroup));
      mapOfGroups.get(routingGroup).registerBackend(backend);
    });

    return new ArrayList<RoutingGroupConfiguration>(mapOfGroups.values());
  }

  /**
   * Adds new routing group.
   * @param group Routing group to add
   */
  public void addRoutingGroup(RoutingGroupConfiguration group) {
    try {
      connectionManager.open();
      RoutingGroups.create(group);
    } finally {
      connectionManager.close();
    }
  }

  /**
   * Updates a routing group and adds it if it does not yet exist.
   * @param group Routing group to update
   */
  public void updateRoutingGroup(RoutingGroupConfiguration group) {
    try {
      connectionManager.open();
      RoutingGroups model = RoutingGroups.findFirst("name = ?", group.getName());

      if (model == null) {
        RoutingGroups.create(group);
      } else {
        RoutingGroups.update(model, group);
      }
    } finally {
      connectionManager.close();
    }
  }
  
  /**
   * Deletes a routing group.
   * @param name Name of routing group to delete
   */
  public void deleteRoutingGroups(String name) {
    try {
      connectionManager.open();
      RoutingGroups.delete(name);
    } finally {
      connectionManager.close();
    }
  }

  /**
   * Pauses a routing group with the specified name.
   * @param name Name of routing group
   */
  public void pauseRoutingGroup(String name) {
    try {
      connectionManager.open();
      RoutingGroups.findFirst("name = ?", name)
                   .set("active", false).saveIt();
    } finally {
      connectionManager.close();
    }
  }

  /**
   * Resumes a routing group with the specified name.
   * @param name Name of routing group
   */
  public void resumeRoutingGroup(String name) {
    try {
      connectionManager.open();
      RoutingGroups.findFirst("name = ?", name)
                   .set("active", true).saveIt();
    } finally {
      connectionManager.close();
    }
  }

  /**
   * Utility function that checks if a routing group with a specified name
   * is active (not paused).
   * @param routingGroups List of routing groups
   * @param name Name of routing group to be searched for
   * @return If the routing group is active
   */
  public final boolean isRoutingGroupActive(List<RoutingGroupConfiguration> routingGroups, 
      String name) {
    return routingGroups.stream().filter(group -> group.getName().equals(name))
              .findFirst().get().isActive();
  }

  /**
   * Utility function that checks if a routing group with a specified name
   * exists within the list and returns it if it exists.
   * @param routingGroups List of routing groups
   * @param name Name of routing group to be searched for
   * @return If the list contains the specified name
   */
  public final RoutingGroupConfiguration getByName(
      List<RoutingGroupConfiguration> routingGroups, 
      String name) {
    Optional<RoutingGroupConfiguration> result = routingGroups.stream()
        .filter(group -> group.getName().equals(name)).findFirst();
    
    return result.isPresent() ? result.get() : null;
  }
}
