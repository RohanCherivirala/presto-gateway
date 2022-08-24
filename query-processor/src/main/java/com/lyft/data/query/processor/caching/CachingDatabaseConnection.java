package com.lyft.data.query.processor.caching;

public abstract class CachingDatabaseConnection {
  public abstract void open();

  public abstract void close();

  public abstract String get(String key);

  public abstract String set(String key, String value);

  public abstract long addToList(String key, String value);

  public abstract boolean addToHash(String key, String hashKey, String hashValue);

  public abstract String getFromHash(String key, String hashKey);

  public abstract boolean validateConnection();

  public abstract void shutdown();
}
