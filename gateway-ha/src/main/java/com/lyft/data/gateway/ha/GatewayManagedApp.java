package com.lyft.data.gateway.ha;

import com.google.inject.Inject;
import com.lyft.data.gateway.ha.caching.CachingDatabaseManager;
import com.lyft.data.server.GatewayServer;

import io.dropwizard.lifecycle.Managed;

public class GatewayManagedApp implements Managed {
  @Inject private GatewayServer gateway;
  @Inject private CachingDatabaseManager cachingDatabaseManager;

  @Override
  public void start() {
    if (gateway != null) {
      gateway.start();
    }

    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.start();
    }
  }

  @Override
  public void stop() {
    if (gateway != null) {
      gateway.close();
    }

    if (cachingDatabaseManager != null) {
      cachingDatabaseManager.shutdown();
    }
  }
}
