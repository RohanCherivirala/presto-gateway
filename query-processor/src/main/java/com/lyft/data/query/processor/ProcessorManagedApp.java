package com.lyft.data.query.processor;

import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;
import com.lyft.data.query.processor.processing.RequestProcessingManager;

import io.dropwizard.lifecycle.Managed;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

/**
 * This class is a managed app that manages all shutdown and startup
 * procedures for the query processor module.
 */
@Slf4j
public class ProcessorManagedApp implements Managed {
  @Inject private CachingDatabaseManager cachingDatabaseManager;
  @Inject private RequestProcessingManager requestManager;
  @Inject private ThreadPoolExecutor queue;

  private ExecutorService singleThreadExecutor = Executors.newCachedThreadPool();

  @Override
  public void start() {
    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.start();
    }

    // Create thread top pick up dropped queries
    singleThreadExecutor.submit(
        () -> {
          while (!QueryProcessor.isTerminated()) {
            String droppedQueryId = cachingDatabaseManager.getFromList(
                CachingDatabaseManager.DROPPED_QUERIES_KEY);

            if (!Strings.isNullOrEmpty(droppedQueryId)) {
              log.debug("Retrieved dropped query [{}]", droppedQueryId);
              requestManager.retryRequest(droppedQueryId);
            }

            try {
              Thread.sleep(QueryProcessor.DROPPED_QUERY_THREAD_SLEEP);
            } catch (InterruptedException e) {
              log.error("Error while sleeping in dropped queries thread");
            }
          }
        });
  }

  @Override
  public void stop() {
    QueryProcessor.indicateShutdown();

    // Stop dropped queries thread
    try {
      singleThreadExecutor.shutdown();
      singleThreadExecutor.awaitTermination(QueryProcessor.DROPPED_QUERY_SHUTDOWN_TIME,
          TimeUnit.SECONDS);

      log.info("Dropped query retrieval thread successfully shut down");
    } catch (InterruptedException e1) {
      log.error("Error occured while shutting down query retrieval thread");
    }

    // Shut down
    queue.shutdown();
    
    // Add all queries that are currently being processed to a hung queries table
    try {
      queue.awaitTermination(QueryProcessor.THREAD_POOL_SHUTDOWN_TIME, TimeUnit.SECONDS);

      while (!QueryProcessor.getQueue().isEmpty()) {
        String nextQueryId = QueryProcessor.getQueue().poll();
        cachingDatabaseManager.addToList(
            CachingDatabaseManager.DROPPED_QUERIES_KEY, nextQueryId);
      }

      log.info("Query processor sucessfully shutdown");
    } catch (InterruptedException e) {
      log.error("Error occured while shutting down thread pool", e);
    }

    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.shutdown();
    }
  }
}
