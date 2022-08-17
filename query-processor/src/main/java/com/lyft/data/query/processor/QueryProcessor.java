package com.lyft.data.query.processor;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.lyft.data.query.processor.config.ClusterRequest;

public class QueryProcessor {
  private volatile static boolean terminated = false;
  public static ConcurrentLinkedQueue<ClusterRequest> queue = new ConcurrentLinkedQueue<>();

  /**
   * Returns if the execution has finished terminating.
   * @return If execution has terminated
   */
  public static boolean isTerminated() {
    return terminated;
  }
}
