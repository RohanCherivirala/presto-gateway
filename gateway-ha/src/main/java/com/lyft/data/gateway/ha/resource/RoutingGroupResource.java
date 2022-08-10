package com.lyft.data.gateway.ha.resource;

import static com.lyft.data.gateway.ha.resource.GatewayResource.isValidName;
import static com.lyft.data.gateway.ha.resource.GatewayResource.throwError;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.router.RoutingGroupsManager;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

/**
 * This class contains endpoints for interacting with routing groups,
 * specifically creating, updating, and pausing them.
 */
@Slf4j
@Path("gateway/routingGroups")
public class RoutingGroupResource {
  @Inject private RoutingGroupsManager routingGroupsManager;
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Endpoint to add a new routing group.
   * @param jsonPayload Data in JSON payload
   * @return Response containing status of query
   */
  @POST
  public Response addRoutingGroup(String jsonPayload) {
    try {
      RoutingGroupConfiguration group = 
          OBJECT_MAPPER.readValue(jsonPayload, RoutingGroupConfiguration.class);
      routingGroupsManager.addRoutingGroup(group);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }

  /**
   * Endpoint to update a routing group.
   * @param jsonPayload Data in JSON payload
   * @return Response containing status of query
   */
  @PUT
  @Path("/{name}")
  public Response updateRoutingGroup(@PathParam("name") String name, String jsonPayload) {
    if (isValidName(name) != null) {
      return isValidName(name);
    }

    try {
      RoutingGroupConfiguration group = 
          OBJECT_MAPPER.readValue(jsonPayload, RoutingGroupConfiguration.class);
      group.setName(name);
      routingGroupsManager.updateRoutingGroup(group);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }

  /**
   * Endpoint to delete a routing group.
   * @param name Name of routing group
   * @return Response containing status of query
   */
  @DELETE
  @Path("/{name}")
  public Response deleteRoutingGroup(@PathParam("name") String name) {
    if (isValidName(name) != null) {
      return isValidName(name);
    }

    try {
      routingGroupsManager.deleteRoutingGroups(name);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }

  /**
   * Endpoint to pause a specific routing group.
   * @param name Name of routing group
   */
  @POST
  @Path("/pauseRoutingGroup/{name}")
  public Response pauseRoutingGroup(@PathParam("name") String name) {
    if (isValidName(name) != null) {
      return isValidName(name);
    }

    try {
      routingGroupsManager.pauseRoutingGroup(name);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }

  /**
   * Endpoint to resume a specific routing group.
   * @name Name of routing group
   */
  @POST
  @Path("/resumeRoutingGroup/{name}")
  public Response resumeRoutingGroup(@PathParam("name") String name) {
    if (isValidName(name) != null) {
      return isValidName(name);
    }
    
    try {
      routingGroupsManager.resumeRoutingGroup(name);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }
}
