package com.lyft.data.gateway.ha.caching;

public abstract class CachingDatabaseConnection {
  public abstract void open();

  public abstract void close();

  public abstract String get(String key);

  public abstract String set(String key, String value);

  public abstract boolean validateConnection();

  public abstract void shutdown();
}
