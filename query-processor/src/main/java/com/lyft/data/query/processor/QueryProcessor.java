package com.lyft.data.query.processor;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;

public class QueryProcessor {
  private static volatile boolean terminated = false;
  private static ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<>();
  private static AsyncHttpClient httpClient = Dsl.asyncHttpClient();

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
   * Returns the async http client.
   * @return Async http client
   */
  public static AsyncHttpClient getHttpClient() {
    return httpClient;
  }

  /**
   * Shutds down query processor application.
   */
  public static void shutdown() {
    terminated = true;
  }
}
