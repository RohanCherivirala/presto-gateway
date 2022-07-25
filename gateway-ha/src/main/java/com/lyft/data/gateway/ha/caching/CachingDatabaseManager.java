package com.lyft.data.gateway.ha.caching;

import com.lyft.data.gateway.ha.config.HaGatewayConfiguration;

public class CachingDatabaseManager {
  public static final String STALL_RESPONSE_SUFFIX = "-Initial-Response";

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

  public String get(String key) {
    return client.get(key);
  }

  public void set(String key, String value) {
    client.set(key, value);
  }

  public void testConnection() {
    client.testConnection();
  }

  public void shutdown() {
    client.shutdown();
  }
}
