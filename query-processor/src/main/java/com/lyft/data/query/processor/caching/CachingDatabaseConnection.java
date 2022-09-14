package com.lyft.data.query.processor.caching;

public abstract class CachingDatabaseConnection {
  public abstract void open();

  public abstract void close();

  public abstract String get(String key);

  public abstract String set(String key, String value);

  public abstract boolean setInHash(String key, String hashKey, String hashValue);

  public abstract String getFromHash(String key, String hashKey);

  public abstract long incrementInHash(String key, String hashKey, int amount);

  public abstract long addToList(String key, String value);

  public abstract String getFromList(String key);

  public abstract long deleteKeys(String... keys);

  public abstract boolean validateConnection();

  public abstract void shutdown();
}
