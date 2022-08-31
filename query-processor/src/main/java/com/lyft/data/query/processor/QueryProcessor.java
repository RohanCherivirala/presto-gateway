package com.lyft.data.query.processor;

import java.util.concurrent.ConcurrentLinkedQueue;

public class QueryProcessor {
  public static int THREAD_POOL_SHUTDOWN_TIME = 40;
  public static int MAX_RETRIES = 2;

  private static volatile boolean terminated = false;
  private static ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();

  /**
   * Returns if the execution has finished terminating.
   * @return If execution has terminated
   */
  public static boolean isTerminated() {
    return terminated;
  }

  /**
   * Returns the concurrent queue for graceful shutdown.
   * @return Concurrent linked queue
   */
  public static ConcurrentLinkedQueue<String> getQueue() {
    return queue;
  }

  /**
   * Indicates that a shutdown is occurring.
   */
  public static void indicateShutdown() {
    terminated = true;
  }
}
