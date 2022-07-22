package com.lyft.data.gateway.ha.caching;

public abstract class CachingDatabaseConnection {
  public abstract void shutdown();

  public abstract void testConnection();
}
