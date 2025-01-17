package com.lyft.data.gateway.ha.resource;

import static com.lyft.data.gateway.ha.resource.GatewayResource.throwError;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;
import com.lyft.data.gateway.ha.router.RoutingGroupsManager;

import io.dropwizard.views.View;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Path("entity")
public class EntityEditorResource {
  @Inject private GatewayBackendManager gatewayBackendManager;
  @Inject private RoutingGroupsManager routingGroupsManager;
  public static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @GET
  @Produces(MediaType.TEXT_HTML)
  public EntityView entityUi() {
    return new EntityView("/template/entity-view.ftl");
  }

  public static class EntityView extends View {
    protected EntityView(String templateName) {
      super(templateName, Charset.defaultCharset());
    }
  }

  private enum EntityType {
    GATEWAY_BACKEND,
    ROUTING_GROUPS
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public List<EntityType> getAllEntityTypes() {
    return Arrays.asList(EntityType.values());
  }

  @POST
  public Response updateEntity(@QueryParam("entityType") String entityTypeStr, String jsonPayload) {
    if (Strings.isNullOrEmpty(entityTypeStr)) {
      throw new WebApplicationException("EntryType can not be null");
    }
    EntityType entityType = EntityType.valueOf(entityTypeStr);
    try {
      switch (entityType) {
        case GATEWAY_BACKEND:
          ProxyBackendConfiguration backend =
              OBJECT_MAPPER.readValue(jsonPayload, ProxyBackendConfiguration.class);
          gatewayBackendManager.updateBackend(backend);
          break;
        case ROUTING_GROUPS:
          RoutingGroupConfiguration group = 
              OBJECT_MAPPER.readValue(jsonPayload, RoutingGroupConfiguration.class);
          routingGroupsManager.updateRoutingGroup(group);
          break;
        default:
      }
    } catch (IOException e) {
      log.error(e.getMessage(), e);
      return throwError(e);
    }

    return Response.ok().build();
  }

  @GET
  @Path("/{entityType}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllEntitiesForType(@PathParam("entityType") String entityTypeStr) {
    EntityType entityType = EntityType.valueOf(entityTypeStr);

    switch (entityType) {
      case GATEWAY_BACKEND:
        return Response.ok(gatewayBackendManager.getAllBackends()).build();
      case ROUTING_GROUPS:
        return Response.ok(routingGroupsManager
            .getAllRoutingGroups(gatewayBackendManager.getAllBackends())).build();
      default:
    }
    return Response.ok(ImmutableList.of()).build();
  }
}
