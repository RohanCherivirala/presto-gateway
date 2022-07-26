package com.lyft.data.gateway.ha.router;

import com.lyft.data.gateway.ha.caching.CachingDatabaseConnection;
import com.lyft.data.gateway.ha.caching.RedisConnection;
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
    try {
      client.open();
      return client.get(key);
    } finally {
      client.close();
    }
  }

  public String set(String key, String value) {
    try {
      client.open();
      return client.set(key, value);
    } finally {
      client.close();
    }
  }

  public boolean validateConnection() {
    return client.validateConnection();
  }

  public void shutdown() {
    client.shutdown();
  }
}
