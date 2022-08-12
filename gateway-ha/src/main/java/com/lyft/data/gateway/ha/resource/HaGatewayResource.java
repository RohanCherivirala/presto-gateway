package com.lyft.data.gateway.ha.resource;

import static com.lyft.data.gateway.ha.resource.GatewayResource.throwError;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;

import java.io.IOException;

import javax.ws.rs.POST;

import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

/**
 * This class creates endpoints that allow for the addition,
 * modification, and deletion of endpoints.
 */
@Slf4j
@Path("gateway/backend/modify")
@Produces(MediaType.APPLICATION_JSON)
public class HaGatewayResource {
  @Inject private GatewayBackendManager haGatewayManager;
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  /**
   * Endpoint to add a backend.
   * @param jsonPayload Data contained in json payload
   * @return Response containing status of operation
   */
  @Path("/add")
  @POST
  public Response addBackend(String jsonPayload) {
    ProxyBackendConfiguration addedBackend;

    try {
      ProxyBackendConfiguration backend = 
          OBJECT_MAPPER.readValue(jsonPayload, ProxyBackendConfiguration.class);
      addedBackend = haGatewayManager.addBackend(backend);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok(addedBackend).build();
  }

  /**
   * Endpoint to update a backend.
   * @param jsonPayload Data contained in json payload
   * @return Response containing status of operation
   */
  @Path("/update")
  @POST
  public Response updateBackend(String jsonPayload) {
    ProxyBackendConfiguration updatedBackend;

    try {
      ProxyBackendConfiguration backend = 
          OBJECT_MAPPER.readValue(jsonPayload, ProxyBackendConfiguration.class);
      updatedBackend = haGatewayManager.updateBackend(backend);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }
    
    return Response.ok(updatedBackend).build();
  }

  /**
   * Endpoint to delete a backend.
   * @param jsonPayload Name of backend to be deleted
   * @return Response containing status of operation
   */
  @Path("/delete")
  @POST
  public Response removeBackend(String name) {
    haGatewayManager.deleteBackend(name);
    return Response.ok().build();
  }
}
