package com.lyft.data.gateway.ha.caching;

public abstract class CachingDatabaseConnection {
  public abstract String get(String key);

  public abstract void set(String key, String value);

  public abstract void testConnection();

  public abstract void shutdown();
}
