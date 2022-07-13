package com.lyft.data.gateway.ha.persistence.dao;

import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;

import java.util.ArrayList;
import java.util.List;

import lombok.extern.slf4j.Slf4j;

import org.javalite.activejdbc.Model;
import org.javalite.activejdbc.annotations.Cached;
import org.javalite.activejdbc.annotations.IdName;
import org.javalite.activejdbc.annotations.Table;

@Slf4j
@Table("routing_groups")
@IdName("name")
@Cached
public class RoutingGroups extends Model {
  private static final String name = "name";
  private static final String active = "active";

  /**
   * Upcasts list of routing groups returned from a query and
   * returns a list of routing group configurations contianing
   * the same information.
   * @param routingGroupList List of routing groups from query
   * @return List of routing group confugurations
   */
  public static List<RoutingGroupConfiguration> upcast(List<RoutingGroups> routingGroupList) {
    List<RoutingGroupConfiguration> routingGroupConfigurations = new ArrayList<>();
    for (RoutingGroups model : routingGroupList) {
      routingGroupConfigurations.add(new RoutingGroupConfiguration(model.getString(name),
                                                                   model.getBoolean(active)));
    }

    return routingGroupConfigurations;
  }

  /**
   * Creates a routing group in the table.
   * @param group Information of routing group
   */
  public static void create(RoutingGroupConfiguration group) {
    RoutingGroups.create(name, group.getName(), active, group.isActive()).insert();
  }

  /**
   * Updates information about a routing group in the table.
   * @param model Model representing table entry of group
   * @param group Information of routing group
   */
  public static void update(RoutingGroups model, RoutingGroupConfiguration group) {
    model.set(name, group.getName(), active, group.isActive()).saveIt();
  }

  /**
   * Deletes a routing group from the table.
   * @param group Information of routing group
   */
  public static void delete(RoutingGroupConfiguration group) {
    RoutingGroups.create(name, group.getName(), active, group.isActive()).delete();
  }

  /**
   * Deletes a routing group from the table by name.
   * @param name Name of routing group to delete.
   */
  public static void delete(String name) {
    RoutingGroups.delete("name = ?", name);
  }
}
