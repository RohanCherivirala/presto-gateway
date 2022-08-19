package com.lyft.data.query.processor;

import com.google.inject.Inject;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;

import io.dropwizard.lifecycle.Managed;

import java.util.concurrent.ThreadPoolExecutor;

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

    QueryProcessor.shutdown();
    queue.shutdown();
  }
}
