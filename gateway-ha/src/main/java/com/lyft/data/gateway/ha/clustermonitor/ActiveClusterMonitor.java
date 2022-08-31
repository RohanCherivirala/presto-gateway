package com.lyft.data.gateway.ha.clustermonitor;

import static com.lyft.data.gateway.ha.handler.QueryIdCachingServerHandler.UI_API_STATS_PATH;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.gateway.ha.config.ProxyBackendConfiguration;
import com.lyft.data.gateway.ha.config.RoutingGroupConfiguration;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;
import com.lyft.data.gateway.ha.router.RoutingGroupsManager;
import com.lyft.data.gateway.ha.router.RoutingManager;

import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

import org.apache.http.HttpStatus;

/**
 * This class creates a thread that runs every 5 seconds
 * and queries all backends and updates the routing
 * table based on that information.
 */
@Slf4j
public class ActiveClusterMonitor implements Managed {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final int BACKEND_CONNECT_TIMEOUT_SECONDS = 15;
  private static final int MONITOR_TASK_DELAY_SECS = 5;

  @Inject private List<PrestoClusterStatsObserver> clusterStatsObservers;
  @Inject private GatewayBackendManager gatewayBackendManager;
  @Inject private RoutingGroupsManager routingGroupsManager;
  @Inject private RoutingManager routingManager;

  private volatile boolean monitorActive = true;

  private OkHttpClient httpClient;
  private ExecutorService executorService = Executors.newFixedThreadPool(10);
  private ExecutorService singleTaskExecutor = Executors.newSingleThreadExecutor();

  /**
   * Run an app that queries all active presto clusters for stats.
   */
  public void start() {
    // Build http client
    OkHttpClient.Builder clientBuilder = new OkHttpClient.Builder();
    clientBuilder.writeTimeout(BACKEND_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    clientBuilder.readTimeout(BACKEND_CONNECT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    httpClient = clientBuilder.build();

    singleTaskExecutor.submit(
        () -> {
          while (monitorActive) {
            try {
              List<ProxyBackendConfiguration> clusters = gatewayBackendManager.getAllBackends();

              List<RoutingGroupConfiguration> routingGroups = routingGroupsManager
                  .getAllRoutingGroups(clusters);

              // Update saved information about routing groups and clusters
              routingManager.updateRoutingGroups(routingGroups);
              routingManager.updateBackendProxyMap(clusters);

              // Service all active cluster in unpaused routing groups
              List<ProxyBackendConfiguration> clustersToService = clusters.stream()
                  .filter(ProxyBackendConfiguration::isActive)
                  .filter(cluster -> {
                    return routingGroupsManager
                           .isRoutingGroupActive(routingGroups, cluster.getRoutingGroup());
                  }).collect(Collectors.toList());

              List<Future<ClusterStats>> futures = new ArrayList<>();
              for (ProxyBackendConfiguration backend : clustersToService) {
                Future<ClusterStats> call =
                    executorService.submit(() -> getPrestoClusterStats(backend));
                futures.add(call);
              }

              List<ClusterStats> stats = new ArrayList<>();
              for (Future<ClusterStats> clusterStatsFuture : futures) {
                ClusterStats clusterStats = clusterStatsFuture.get();
                stats.add(clusterStats);
              }

              if (clusterStatsObservers != null) {
                for (PrestoClusterStatsObserver observer : clusterStatsObservers) {
                  observer.observe(stats);
                }
              }
            } catch (Exception e) {
              log.error("Error performing backend monitor tasks", e);
            }

            try {
              Thread.sleep(MONITOR_TASK_DELAY_SECS * 1000);
            } catch (Exception e) {
              log.error("Error with monitor task", e);
            }
          }
        });
  }

  /**
   * Sends an HTTP request to a backend to get information about
   * the current status of the backend and returns it.
   * 
   * @param backend Backend to get information about
   * @return A {@link ClusterStats} variable with information about the backend
   */
  private ClusterStats getPrestoClusterStats(ProxyBackendConfiguration backend) {
    ClusterStats clusterStats = new ClusterStats();
    clusterStats.setClusterId(backend.getName());
    clusterStats.setHealthy(false);

    // The V1_NODE_PATH is used in 331 while V1_CLUSTER_PATH is used in 318
    // TODO: Remove V1_CLUSTER_PATH once we're upgraded all clusters.

    String dynpath = ""; // Path Based on Presto and Trino cluster

    if (backend.getProxyTo().contains("trino") || backend.getProxyTo().contains("dashboard")) {
      dynpath = UI_API_STATS_PATH;
    } else {
      dynpath = "/v1/cluster";
    }

    String target = backend.getProxyTo() + dynpath;
    
    try {
      // Build http request
      Request request = new Request.Builder()
          .get()
          .url(target)
          .build();
      
      // Send http request
      Response response = httpClient.newCall(request).execute();

      // Parse response
      if (response.code() == HttpStatus.SC_OK) {
        clusterStats.setHealthy(true);

        HashMap<String, Object> result = 
            OBJECT_MAPPER.readValue(response.body().string(), HashMap.class);

        clusterStats.setNumWorkerNodes((int) result.get("activeWorkers"));
        clusterStats.setQueuedQueryCount((int) result.get("queuedQueries"));
        clusterStats.setRunningQueryCount((int) result.get("runningQueries"));
        clusterStats.setBlockedQueryCount((int) result.get("blockedQueries"));
        clusterStats.setProxyTo(backend.getProxyTo());
        clusterStats.setRoutingGroup(backend.getRoutingGroup());
        log.info("Host: {}, Cluster_stat: {}", System.getenv("HOSTNAME"), clusterStats);
      } else {
        log.error("Received non 200 response, response code: " 
            + "{} when fetching cluster stats from [{}]", response.code(), target);
      }
    } catch (Exception e) {
      clusterStats.setHealthy(false);
      log.error("Error fetching cluster stats from [{}]", target, e);
    }

    return clusterStats;
  }

  /**
   * Shut down the app.
   */
  public void stop() {
    this.monitorActive = false;
    this.executorService.shutdown();
    this.singleTaskExecutor.shutdown();
  }
}
