package com.lyft.data.gateway.ha.resource;

import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingGroupsManager;

import io.dropwizard.views.View;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * This class provides the HTML produced when somebody visites the
 * web page and also creates multiple API endpoints for
 * communicaiton.
 */
@Path("/")
public class GatewayViewResource {
  private static final long START_TIME = System.currentTimeMillis();
  @Inject private GatewayBackendManager gatewayBackendManager;
  @Inject private QueryHistoryManager queryHistoryManager;
  @Inject private RoutingGroupsManager routingGroupsManager;

  /**
   * This function creates and returns a view that represents the homepage
   * of the website, which is the query history page. The associated ftl template
   * lives at "/template/query-history-view.ftl".
   * 
   * @return A view associated with the homepage (query details and history)
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public GatewayView getQueryDetailsView() {
    GatewayView queryHistoryView = new GatewayView("/template/query-history-view.ftl");
    fillGatewayViewWithInfo(queryHistoryView);
    
    return queryHistoryView;
  }

  /**
   * This function creates and returns a view that represents the view gateway
   * page on the website. The associated ftl template lives at
   * "/template/gateway-view.ftl".
   * 
   * <p>This page is accessible at "/viewgateway".
   * @return A view for the viewGateway page
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  @Path("viewgateway")
  public GatewayView getGatewayView() {
    GatewayView gatewayView = new GatewayView("/template/gateway-view.ftl");
    fillGatewayViewWithInfo(gatewayView);
    
    return gatewayView;
  }

  /**
   * This function creates and returns a view that represents the routing groups
   * on the website. The associated ftl template with lives at
   * "/template/routing-groupts-view.ftl".
   * 
   * <p>This information is accessible at "/routingGroups".
   * @return A view for the routingGroups page
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  @Path("routingGroups")
  public GatewayView getRoutingGroupView() {
    GatewayView routingGroupsView = new GatewayView("/template/routing-groups-view.ftl");
    fillGatewayViewWithInfo(routingGroupsView);

    return routingGroupsView;
  }

  /**
   * This function gets the query history.
   * 
   * <p>This information is accessible at "/api/queryHistory".
   * @return A list of the query history
   */
  @GET
  @Path("api/queryHistory")
  @Produces(MediaType.APPLICATION_JSON)
  public List<QueryHistoryManager.QueryDetail> getQueryHistory() {
    return queryHistoryManager.fetchQueryHistory();
  }

  /**
   * This functions gets all active backends.
   * 
   * <p>This information is accessible at "/api/activeBackends".
   * @return A list of all active backends
   */
  @GET
  @Path("api/activeBackends")
  @Produces(MediaType.APPLICATION_JSON)
  public List<ProxyBackendConfiguration> getActiveBackends() {
    return gatewayBackendManager.getAllBackends().stream()
        .filter(ProxyBackendConfiguration::isActive)
        .collect(Collectors.toList());
  }

  /**
   * This function gets all routing groups.
   * 
   * <p>This information is accessible at "/api/routingGroups".
   * @return A list of routing groups
   */
  @GET
  @Path("api/routingGroups")
  @Produces(MediaType.APPLICATION_JSON)
  public List<RoutingGroupConfiguration> getRoutingGroups() {
    List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();
    return routingGroupsManager.getAllRoutingGroups(backends);
  }

  /**
   * This function returns a mapping of each backend to the number
   * of queries it has processed.
   * 
   * <p>This information is accessible at "/api/queryHistoryDistribution".
   * @return A map containing backends and the number of queries it processed
   */
  @GET
  @Path("api/queryHistoryDistribution")
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, Integer> getQueryHistoryDistribution() {
    Map<String, String> urlToNameMap = new HashMap<>();
    gatewayBackendManager
        .getAllBackends()
        .forEach(
            backend -> {
              urlToNameMap.put(backend.getProxyTo(), backend.getName());
            });

    Map<String, Integer> clusterToQueryCount = new HashMap<>();
    queryHistoryManager
        .fetchQueryHistory()
        .forEach(
            q -> {
              String backend = urlToNameMap.get(q.getBackendUrl());
              if (backend == null) {
                backend = q.getBackendUrl();
              }

              clusterToQueryCount.putIfAbsent(backend, 0);
              clusterToQueryCount.put(backend, clusterToQueryCount.get(backend) + 1);
            });
            
    return clusterToQueryCount;
  }

  /**
   * This function fills a gateway view with relavent information.
   * @param view View to be filled in
   */
  private void fillGatewayViewWithInfo(GatewayView view) {
    List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();
    view.setBackendConfigurations(backends);
    view.setRoutingGroupConfigurations(routingGroupsManager
        .getAllRoutingGroups(backends));

    view.setQueryHistory(queryHistoryManager.fetchQueryHistory());
    view.setQueryDistribution(getQueryHistoryDistribution());
  }

  /**
   * This class holds the data that is passed into the ftl templates.
   */
  @Data
  @EqualsAndHashCode(callSuper = false)
  public static class GatewayView extends View {
    private final long gatewayStartTime = START_TIME;
    private List<ProxyBackendConfiguration> backendConfigurations;
    private List<RoutingGroupConfiguration> routingGroupConfigurations;
    private List<QueryHistoryManager.QueryDetail> queryHistory;
    private Map<String, Integer> queryDistribution;

    protected GatewayView(String templateName) {
      super(templateName, Charset.defaultCharset());
    }
  }
}
