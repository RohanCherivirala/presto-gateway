package com.lyft.data.server;

import com.google.inject.Inject;

import io.dropwizard.lifecycle.Managed;

import java.util.concurrent.ScheduledThreadPoolExecutor;

public class ServerManagedApp implements Managed {
  @Inject
  ScheduledThreadPoolExecutor scheduleThreadPool;

  @Override
  public void start() throws Exception {
    // Do nothing on startup
  }

  @Override
  public void stop() throws Exception {
    scheduleThreadPool.shutdown();
  }
}
