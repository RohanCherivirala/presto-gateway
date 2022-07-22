package com.lyft.data.gateway.ha.caching;

import com.lyft.data.gateway.ha.config.HaGatewayConfiguration;

public class CachingDatabaseManager {
  private HaGatewayConfiguration configuration;
  private CachingDatabaseConnection client;

  public CachingDatabaseManager(HaGatewayConfiguration configuration) {
    this.configuration = configuration;
  }

  public void start() {
    if (configuration.getCachingDatabase().getDatabaseType().equalsIgnoreCase("redis")) {
      client = new RedisConnection(configuration);
    }
  }

  public void testConnection() {
    client.testConnection();
  }

  public void shutdown() {
    client.shutdown();
  }
}
