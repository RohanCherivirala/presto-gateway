package com.lyft.data.query.processor;

import com.google.inject.Inject;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;

import io.dropwizard.lifecycle.Managed;

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
  @Inject private ThreadPoolExecutor queue;

  @Override
  public void start() {
    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.start();
    }
  }

  @Override
  public void stop() {
    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.shutdown();
    }

    QueryProcessor.indicateShutdown();
    queue.shutdown();
    
    // Add all queries that are currently being processed to a hung queries table
    try {
      queue.awaitTermination(QueryProcessor.THREAD_POOL_SHUTDOWN_TIME, TimeUnit.SECONDS);

      while (!QueryProcessor.getQueue().isEmpty()) {
        String nextQueryId = QueryProcessor.getQueue().poll();
        cachingDatabaseManager.addToList(
            CachingDatabaseManager.HUNG_QUERIES_KEY, nextQueryId);
      }

      log.info("Query Processor Sucessfully Shutdown");
    } catch (InterruptedException e) {
      log.error("Error occured while shutting down thread pool", e);
    }
  }
}
