package com.lyft.data.gateway.ha.router;

import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;

/**
 * A Routing Manager that provides load distribution on to registered active backends based on
 * its queue length. This manager listens to updates on cluster queue length as recorded by the
 * {@link com.lyft.data.gateway.ha.clustermonitor.ActiveClusterMonitor}
 * Ideally where ever a modification is made to the the list of backends(adding, removing) this
 * routing manager should get notified & updated.
 * Currently updates are made only on heart beats from
 * {@link com.lyft.data.gateway.ha.clustermonitor.ActiveClusterMonitor} & during routing requests.
 */
@Slf4j
public class PrestoQueueLengthRoutingTable extends HaRoutingManager {
  private static final Random RANDOM = new Random();
  private static final int MIN_WT = 1;
  private static final int MAX_WT = 100;
  private ConcurrentHashMap<String, Integer> routingGroupWeightSum;
  private ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>> clusterQueueLengthMap;
  private Map<String, TreeMap<Integer, String>> weightedDistributionRouting;

  /**
   * A Routing Manager that distributes queries according to assigned weights based on
   * Presto cluster queue length.
   */
  public PrestoQueueLengthRoutingTable(GatewayBackendManager gatewayBackendManager,
                                       QueryHistoryManager queryHistoryManager,
                                       RoutingGroupsManager routingGroupsManager) {
    super(gatewayBackendManager, queryHistoryManager, routingGroupsManager);
    routingGroupWeightSum = new ConcurrentHashMap<String, Integer>();
    clusterQueueLengthMap = new ConcurrentHashMap<String, ConcurrentHashMap<String, Integer>>();
    weightedDistributionRouting = new HashMap<String, TreeMap<Integer, String>>();
  }

  /**
   * All wts are assigned as a fraction of maxQueueLn. Cluster with maxQueueLn should be
   * given least weightage. What this value should be depends on the queueing on the rest of
   * the clusters. since the list is sorted, the smallestQueueLn & lastButOneQueueLn give us
   * the range. In an ideal situation all clusters should have equals distribution of
   * queries, hence that is used as a threshold to check is a cluster queue is over
   * provisioned or not.
   */
  private int getWeightForMaxQueueCluster(LinkedHashMap<String, Integer> sortedByQueueLength) {
    int queueSum = 0;
    int numBuckets = 1;
    int equalDistribution = 0;
    int calculatedWtMaxQueue = 0;

    numBuckets = sortedByQueueLength.size();
    queueSum = sortedByQueueLength.values().stream().mapToInt(Integer::intValue).sum();
    equalDistribution = queueSum / numBuckets;

    Object[] queueLengths = sortedByQueueLength.values().toArray();

    int smallestQueueLn = (Integer) queueLengths[0];
    int maxQueueLn = (Integer) queueLengths[queueLengths.length - 1];
    int lastButOneQueueLn = smallestQueueLn;
    if (queueLengths.length > 2) {
      lastButOneQueueLn = (Integer) queueLengths[queueLengths.length - 2];
    }

    if (maxQueueLn == 0) {
      calculatedWtMaxQueue = MAX_WT;
    } else if (lastButOneQueueLn == 0 || (lastButOneQueueLn == maxQueueLn)) {
      calculatedWtMaxQueue = MIN_WT;
    } else {
      int lastButOneQueueWt =
          (int) Math.ceil((MAX_WT - (lastButOneQueueLn * MAX_WT / (double) maxQueueLn)));
      double fractionOfLastWt = (smallestQueueLn / (float) maxQueueLn);
      calculatedWtMaxQueue = (int) Math.ceil(fractionOfLastWt * lastButOneQueueWt);

      if (lastButOneQueueLn < equalDistribution
          || (lastButOneQueueLn > equalDistribution && smallestQueueLn <= equalDistribution)) {
        calculatedWtMaxQueue = (smallestQueueLn == 0) ? MIN_WT :
            (int) Math.ceil(fractionOfLastWt * fractionOfLastWt * lastButOneQueueWt);
      }
    }

    return calculatedWtMaxQueue;
  }

  /**
   * Uses the queue length of a cluster to assign weights to all active clusters in a routing group.
   * The weights assigned ensure a fair distribution of routing for queries such that clusters with
   * the least queue length get assigned more queries.
   */
  private void computeWeightsBasedOnQueueLength(ConcurrentHashMap<String,
      ConcurrentHashMap<String, Integer>> queueLengthMap) {
    synchronized (lockObject) {
      int sum = 0;
      int weight;
      int numBuckets = 1;
      int maxQueueLn = 0;
      int calculatedWtMaxQueue = 0;

      weightedDistributionRouting.clear();
      routingGroupWeightSum.clear();

      log.debug("Computing Weights for Queue Map :[{}] ", queueLengthMap.toString());

      for (String routingGroup : queueLengthMap.keySet()) {
        sum = 0;
        TreeMap<Integer, String> weightsMap = new TreeMap<>();

        if (queueLengthMap.get(routingGroup).size() == 0) {
          log.warn("No active clusters in routingGroup : [{}]. Continue to "
              + "process rest of routing table ", routingGroup);
          continue;
        } else if (queueLengthMap.get(routingGroup).size() == 1) {
          log.debug("Routing Group: [{}] has only 1 active backend.", routingGroup);
          weightedDistributionRouting.put(routingGroup, new TreeMap<Integer, String>() {
                {
                  put(MAX_WT, queueLengthMap.get(routingGroup).keys().nextElement());
                }
              }
          );
          routingGroupWeightSum.put(routingGroup, MAX_WT);
          continue;
        }

        LinkedHashMap<String, Integer> sortedByQueueLength = queueLengthMap.get(routingGroup)
            .entrySet()
            .stream().sorted(Comparator.comparing(Map.Entry::getValue))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                (e1, e2) -> e1, LinkedHashMap::new));

        numBuckets = sortedByQueueLength.size();

        Object[] queueLengths = sortedByQueueLength.values().toArray();
        Object[] clusterNames = sortedByQueueLength.keySet().toArray();

        maxQueueLn = (Integer) queueLengths[queueLengths.length - 1];
        calculatedWtMaxQueue = getWeightForMaxQueueCluster(sortedByQueueLength);

        for (int i = 0; i < numBuckets - 1; i++) {
          // If all clusters have same queue length, assign same wt
          weight = (maxQueueLn == (Integer) queueLengths[i]) ? calculatedWtMaxQueue :
              (int) Math.ceil(MAX_WT
                  - (((Integer) queueLengths[i] * MAX_WT) / (double) maxQueueLn));
          sum += weight;
          weightsMap.put(sum, (String) clusterNames[i]);
        }

        sum += calculatedWtMaxQueue;
        weightsMap.put(sum, (String) clusterNames[numBuckets - 1]);

        weightedDistributionRouting.put(routingGroup, weightsMap);
        routingGroupWeightSum.put(routingGroup, sum);
      }

      if (log.isDebugEnabled()) {
        for (String rg : weightedDistributionRouting.keySet()) {
          log.debug("Routing Table for : [{}] is [{}]", rg,
              weightedDistributionRouting.get(rg).toString());
        }
      }
    }
  }

  /**
   * Update the Routing Table only if a previously known backend has been deactivated.
   * Newly added backends are handled through
   * {@link PrestoQueueLengthRoutingTable#updateRoutingTable(Map)}
   * updateRoutingTable}
   */
  public void updateRoutingTable(String routingGroup, Set<String> backends) {
    synchronized (lockObject) {
      if (clusterQueueLengthMap.containsKey(routingGroup)) {
        log.debug("Update routing table for routing group : [{}]"
            + " with active backends : [{}]", routingGroup, backends.toString());
        Collection<String> knownBackends = new HashSet<String>();
        knownBackends.addAll(clusterQueueLengthMap.get(routingGroup).keySet());

        if (backends.containsAll(knownBackends)) {
          return;
        } else {
          if (knownBackends.removeAll(backends)) {
            for (String inactiveBackend : knownBackends) {
              clusterQueueLengthMap.get(routingGroup).remove(inactiveBackend);
            }
          }
        }
      }

      computeWeightsBasedOnQueueLength(clusterQueueLengthMap);
    }
  }

  /**
   * Update routing Table with new Queue Lengths.
   */
  public void updateRoutingTable(Map<String, Map<String, Integer>> updatedQueueLengthMap) {
    synchronized (lockObject) {
      log.debug("Update Routing table with new cluster queue lengths : [{}]",
          updatedQueueLengthMap.toString());
      clusterQueueLengthMap.clear();

      for (String grp : updatedQueueLengthMap.keySet()) {
        ConcurrentHashMap<String, Integer> queueMap = new ConcurrentHashMap<>();
        queueMap.putAll(updatedQueueLengthMap.get(grp));
        clusterQueueLengthMap.put(grp, queueMap);
      }

      computeWeightsBasedOnQueueLength(clusterQueueLengthMap);
    }
  }

  /**
   * A convenience method to peak into the weights used by the routing Manager.
   */
  public Map<String, Integer> getInternalWeightedRoutingTable(String routingGroup) {
    if (!weightedDistributionRouting.containsKey(routingGroup)) {
      return null;
    }
    Map<String, Integer> routingTable = new HashMap<>();

    for (Integer wt : weightedDistributionRouting.get(routingGroup).keySet()) {
      routingTable.put(weightedDistributionRouting.get(routingGroup).get(wt), wt);
    }
    return routingTable;
  }

  /**
   * A convienience method to get a peak into the state of the routing manager.
   */
  public Map<String, Integer> getInternalClusterQueueLength(String routingGroup) {
    if (!clusterQueueLengthMap.containsKey(routingGroup)) {
      return null;
    }

    return clusterQueueLengthMap.get(routingGroup);
  }

  /**
   * Looks up the closest weight to random number generated for a given routing group.
   */
  public String getEligibleBackEnd(String routingGroup) {
    if (routingGroupWeightSum.containsKey(routingGroup)
        && weightedDistributionRouting.containsKey(routingGroup)) {
      int rnd = RANDOM.nextInt(routingGroupWeightSum.get(routingGroup));
      return weightedDistributionRouting.get(routingGroup).higherEntry(rnd).getValue();
    } else {
      return null;
    }
  }

  /**
   * Routes a query in the routing group and excludes the backend that the query
   * was previously sent to.
   * @param routingGroup Routing group to send to
   * @param exclude Name of backend to avoid
   * @return Name of backend to route to
   */
  public String getEligibleBackEnd(String routingGroup, String exclude) {
    if (routingGroupWeightSum.containsKey(routingGroup)
        && weightedDistributionRouting.containsKey(routingGroup)) {
      TreeMap<Integer, String> mapWithoutBackend = new TreeMap<>();

      int sum = 0;
      for (Entry<Integer, String> entry 
          : weightedDistributionRouting.get(routingGroup).entrySet()) {
        if (!entry.getValue().equals(exclude)) {
          sum += entry.getKey().intValue();
          mapWithoutBackend.put(sum, entry.getValue());
        }
      }

      int rnd = RANDOM.nextInt(sum);
      return mapWithoutBackend.higherEntry(rnd).getValue();
    } else {
      return null;
    }
  }

  /**
   * Provide a backend to be routed to that does not include the backend that
   * was previously routed to.
   * @param routingGroups Routing group to rout to
   * @param lastBackend The last backend that processed the response
   * @return The backend address to send the request to
   */
  public String provideBackendForRoutingGroup(String routingGroup, String lastBackend) {
    Map<String, Integer> backends = clusterQueueLengthMap.get(routingGroup);

    if (backends == null || backends.isEmpty()
        || !routingGroups.get(routingGroup)) {
      log.debug("Routing group {} is currently paused or has no active backends", routingGroup);
      return provideAdhocBackend();
    }
    
    String lastBackendName = "";
    if (!Strings.isNullOrEmpty(lastBackend) && backends.size() > 1) {
      for (Entry<String, String> entry : backendProxyMap.entrySet()) {
        if (entry.getValue().equals(lastBackend)) {
          lastBackendName = entry.getKey();
          break;
        }
      }
    }

    String clusterId = Strings.isNullOrEmpty(lastBackendName)
        ? getEligibleBackEnd(routingGroup)
        : getEligibleBackEnd(routingGroup, lastBackendName);

    log.debug("Routing to eligible backend : [{}] for routing group: [{}]",
        clusterId, routingGroup);

    if (clusterId != null) {
      return backendProxyMap.get(clusterId);
    } else {
      log.debug("Falling back to random distribution");
      String randomClusterId = new ArrayList<String>(backends.keySet())
          .get(RANDOM.nextInt(backends.size()));
      return backendProxyMap.get(randomClusterId);
    }
  }

  /**
   * Performs routing to a given cluster group. This falls back to an adhoc backend, if no scheduled
   * backend is found.
   */
  @Override
  public String provideBackendForRoutingGroup(String routingGroup) {
    return provideBackendForRoutingGroup(routingGroup, null);
  }

  /**
   * Performs routing to an adhoc backend based on computed weights.
   *
   * <p>d.
   */
  @Override
  public String provideAdhocBackend() {
    Map<String, Integer> backends = clusterQueueLengthMap.get(ADHOC);
    
    if (backends == null || backends.size() == 0) {
      throw new IllegalStateException("Number of active backends found zero");
    }

    if (!routingGroups.get(ADHOC)) {
      throw new IllegalStateException(
          "All available backends are currently undergoing maintainence");
    }

    String clusterId = getEligibleBackEnd(ADHOC);
    log.debug("Routing to eligible backend : " + clusterId + " for routing group: adhoc");
    if (clusterId != null) {
      return backendProxyMap.get(clusterId);
    } else {
      log.debug("Falling back to random distribution");
      String randomClusterId = new ArrayList<String>(backends.keySet())
          .get(RANDOM.nextInt(backends.size()));
      return backendProxyMap.get(randomClusterId);
    }
  }
}
