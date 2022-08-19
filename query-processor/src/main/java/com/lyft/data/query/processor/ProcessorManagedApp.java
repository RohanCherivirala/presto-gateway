package com.lyft.data.query.processor;

import java.util.concurrent.ThreadPoolExecutor;

import com.google.inject.Inject;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;

import io.dropwizard.lifecycle.Managed;

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

    queue.shutdown();
  }
}
