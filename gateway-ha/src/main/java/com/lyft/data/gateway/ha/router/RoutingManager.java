package com.lyft.data.gateway.ha.router;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.proxyserver.ProxyServerConfiguration;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;

/**
 * This class performs health check, stats counts for each backend and provides a backend given
 * request object. Default implementation comes here.
 */
@Slf4j
public abstract class RoutingManager {
  public static final String ADHOC = "adhoc";
  
  private static final Random RANDOM = new Random();
  private final LoadingCache<String, String> queryIdBackendCache;
  private ExecutorService executorService = Executors.newFixedThreadPool(5);
  private GatewayBackendManager gatewayBackendManager;
  protected RoutingGroupsManager routingGroupsManager;

  protected final Object lockObject = new Object();
  public ConcurrentHashMap<String, Boolean> routingGroups = new ConcurrentHashMap<>();
  public ConcurrentHashMap<String, String> backendProxyMap = new ConcurrentHashMap<>();

  public RoutingManager(GatewayBackendManager gatewayBackendManager,
      RoutingGroupsManager routingGroupsManager) {
    this.gatewayBackendManager = gatewayBackendManager;
    this.routingGroupsManager = routingGroupsManager;
    queryIdBackendCache =
        CacheBuilder.newBuilder()
            .maximumSize(10000)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build(
                new CacheLoader<String, String>() {
                  @Override
                  public String load(String queryId) {
                    return findBackendForUnknownQueryId(queryId);
                  }
                });
  }

  protected GatewayBackendManager getGatewayBackendManager() {
    return gatewayBackendManager;
  }

  public void setBackendForQueryId(String queryId, String backend) {
    queryIdBackendCache.put(queryId, backend);
  }

  /**
   * Updates the internal routing groups.
   * @param groups Routing groups
   */
  public void updateRoutingGroups(List<RoutingGroupConfiguration> groups) {
    synchronized (lockObject) {
      routingGroups.clear();
      for (RoutingGroupConfiguration group : groups) {
        routingGroups.put(group.getName(), group.isActive());
      }
    }
  }

  /**
   * Updates the backend map.
   * @param map Updated map of backends.
   */
  public void updateBackendProxyMap(List<ProxyBackendConfiguration> clusters) {
    synchronized (lockObject) {
      backendProxyMap.clear();
      for (ProxyBackendConfiguration cluster : clusters) {
        backendProxyMap.put(cluster.getName(), cluster.getProxyTo());
      }
    }
  }

  /**
   * Performs routing to an adhoc backend and checks if it is not paused.
   *
   * <p>d.
   *
   * @return
   */
  public String provideAdhocBackend() {
    List<ProxyBackendConfiguration> backends = this.gatewayBackendManager.getActiveAdhocBackends();

    if (backends.size() == 0) {
      throw new IllegalStateException("No adhoc backends are currently available");
    }

    if (!routingGroups.get("adhoc")) {
      throw new IllegalStateException(
          "All available backends are currently undergoing maintainence");
    }

    int backendId = Math.abs(RANDOM.nextInt()) % backends.size();
    return backends.get(backendId).getProxyTo();
  }

  /**
   * Performs routing to a given cluster group. This falls back to an adhoc backend, if no scheduled
   * backend is found or if the routing group is paused.
   *
   * @return
   */
  public String provideBackendForRoutingGroup(String routingGroup) {
    List<ProxyBackendConfiguration> backends =
        gatewayBackendManager.getActiveBackends(routingGroup);

    if (backends.isEmpty()
        || !routingGroups.get(routingGroup)) {
      log.warn("Routing group {} is currently paused or has no active backends, "
          + "routing {} to adhoc", routingGroup);
      return provideAdhocBackend();
    }

    int backendId = Math.abs(RANDOM.nextInt()) % backends.size();
    return backends.get(backendId).getProxyTo();
  }

  /**
   * Performs cache look up, if a backend not found, it checks with all backends and tries to find
   * out which backend has info about given query id.
   *
   * @param queryId
   * @return
   */
  public String findBackendForQueryId(String queryId) {
    String backendAddress = null;
    try {
      backendAddress = queryIdBackendCache.get(queryId);
    } catch (ExecutionException e) {
      log.error("Exception while loading queryId from cache {}", e.getLocalizedMessage());
    }
    return backendAddress;
  }

  /**
   * This tries to find out which backend may have info about given query id. If not found returns
   * the first healthy backend.
   *
   * @param queryId
   * @return
   */
  protected String findBackendForUnknownQueryId(String queryId) {
    List<ProxyBackendConfiguration> backends = gatewayBackendManager.getAllBackends();

    Map<String, Future<Integer>> responseCodes = new HashMap<>();
    try {
      for (ProxyServerConfiguration backend : backends) {
        String target = backend.getProxyTo() + "/v1/query/" + queryId;

        Future<Integer> call =
            executorService.submit(
                () -> {
                  URL url = new URL(target);
                  HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                  conn.setConnectTimeout((int) TimeUnit.SECONDS.toMillis(5));
                  conn.setReadTimeout((int) TimeUnit.SECONDS.toMillis(5));
                  conn.setRequestMethod(HttpMethod.HEAD);
                  return conn.getResponseCode();
                });
        responseCodes.put(backend.getProxyTo(), call);
      }
      for (Map.Entry<String, Future<Integer>> entry : responseCodes.entrySet()) {
        if (entry.getValue().isDone()) {
          int responseCode = entry.getValue().get();
          if (responseCode == 200) {
            log.info("Found query [{}] on backend [{}]", queryId, entry.getKey());
            setBackendForQueryId(queryId, entry.getKey());
            return entry.getKey();
          }
        }
      }
    } catch (Exception e) {
      log.warn("Query id [{}] not found", queryId);
    }
    // Fallback on first active backend if queryId mapping not found.
    return gatewayBackendManager.getActiveAdhocBackends().get(0).getProxyTo();
  }
}
