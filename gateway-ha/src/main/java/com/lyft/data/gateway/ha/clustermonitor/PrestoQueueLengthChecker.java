package com.lyft.data.gateway.ha.clustermonitor;

import com.lyft.data.gateway.ha.router.PrestoQueueLengthRoutingTable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Updates the QueueLength Based Routing Manager
 * {@link PrestoQueueLengthRoutingTable} with
 * updated queue lengths of active clusters.
 */
public class PrestoQueueLengthChecker implements PrestoClusterStatsObserver {

  PrestoQueueLengthRoutingTable routingManager;

  public PrestoQueueLengthChecker(PrestoQueueLengthRoutingTable routingManager) {
    this.routingManager = routingManager;
  }

  /**
   * Creates a mapping of routing group to clusterID and query count
   * in order to update the routing table.
   * 
   * @param stats List of cluster stats
   */
  @Override
  public void observe(List<ClusterStats> stats) {
    Map<String, Map<String, Integer>> clusterQueueMap = 
        new HashMap<String, Map<String, Integer>>();

    for (ClusterStats stat : stats) {
      // Only add healthy clusters to be routed to that have active workers
      if (stat.isHealthy() && stat.getNumWorkerNodes() > 0) {
        clusterQueueMap.putIfAbsent(stat.getRoutingGroup(), new HashMap<String, Integer>());

        clusterQueueMap.get(stat.getRoutingGroup())
                       .put(stat.getClusterId(), stat.getQueuedQueryCount());
      }
    }

    routingManager.updateRoutingTable(clusterQueueMap);
  }
}
