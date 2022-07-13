package com.lyft.data.gateway.ha.resource;

import com.google.inject.Inject;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;
import com.lyft.data.gateway.ha.router.RoutingGroupsManager;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Request;
import javax.ws.rs.core.Response;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("/gateway")
@Produces(MediaType.APPLICATION_JSON)
public class GatewayResource {
  @Inject private GatewayBackendManager gatewayBackendManager;
  @Inject private RoutingGroupsManager routingGroupsManager;

  @GET
  public Response ok(@Context Request request) {
    return Response.ok("ok").build();
  }

  /**
   * Gets all backends.
   */
  @GET
  @Path("/backend/all")
  public Response getAllBackends() {
    return Response.ok(this.gatewayBackendManager.getAllBackends()).build();
  }

  /**
   * Gets all active backends.
   */
  @GET
  @Path("/backend/active")
  public Response getActiveBackends() {
    return Response.ok(gatewayBackendManager.getAllActiveBackends()).build();
  }

  /**
   * Deactivate a specific cluster.
   * @name Name of cluster
   */
  @POST
  @Path("/backend/deactivate/{name}")
  public Response deactivateBackend(@PathParam("name") String name) {
    try {
      this.gatewayBackendManager.deactivateBackend(name);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }
    return Response.ok().build();
  }

  /**
   * Activate a specific cluster.
   * @name Name of cluster
   */
  @POST
  @Path("/backend/activate/{name}")
  public Response activateBackend(@PathParam("name") String name) {
    try {
      this.gatewayBackendManager.activateBackend(name);
    } catch (Exception e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }
    return Response.ok().build();
  }

  /**
   * Method to throw an error response.
   * @param e Error called
   * @return Response containing error
   */
  public static Response throwError(Exception e) {
    return Response.status(Response.Status.UNSUPPORTED_MEDIA_TYPE)
        .build();
  }
}
