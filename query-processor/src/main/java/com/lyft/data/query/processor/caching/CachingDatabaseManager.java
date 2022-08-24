package com.lyft.data.query.processor.caching;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

/**
 * This class serves to manage a connection with a caching database.
 */
public class CachingDatabaseManager {
  public static final String ACTIVE_QUERIES_PREFIX = "active-queries/";
  public static final String QUERY_CACHE_PREFIX = "query-cache/";

  public static final String INITIAL_REQUEST_SUFFIX = ":initial-request";
  public static final String INITIAL_RESPONSE_SUFFIX = ":initial-response";
  public static final String CACHED_RESONSE_SUFFIX = ":cache";
  public static final String COMPLETION_SUFFIX = ":completed";

  public static final String HEADER_FIELD = "header";
  public static final String BODY_FIELD = "body";

  public static final String HUNG_QUERIES_KEY = ACTIVE_QUERIES_PREFIX + "hung-queries";

  private QueryProcessorConfiguration configuration;
  private CachingDatabaseConnection client;

  /**
   * Constructor for a caching database manager.
   * @param configuration Configuration of caching database
   */
  public CachingDatabaseManager(QueryProcessorConfiguration configuration) {
    this.configuration = configuration;
  }

  /**
   * Configure a connection with the caching database.
   */
  public void start() {
    if (configuration.getCachingDatabase() != null
        && configuration.getCachingDatabase().getDatabaseType().equalsIgnoreCase("redis")) {
      client = new RedisConnection(configuration);
    }
  }
  
  /**
   * Get a key from the caching database.
   * @param key Key to search
   * @return Value associated with key
   */
  public String get(String key) {
    try {
      client.open();
      return client.get(key);
    } finally {
      client.close();
    }
  }

  /**
   * Sets a key to a specific value.
   * @param key Key to set
   * @param value Value to set key to
   * @return Set return value
   */
  public String set(String key, String value) {
    try {
      client.open();
      return client.set(key, value);
    } finally {
      client.close();
    }
  }

  /**
   * Adds a key to a list.
   * @param key Redis key
   * @param value Value to add to list
   * @return List addition return value
   */
  public long addToList(String key, String value) {
    try {
      client.open();
      return client.addToList(key, value);
    } finally {
      client.close();
    }
  }

  /**
   * Adds a key-value mapping to a hash.
   * @param key Key to use
   * @param hashKey Key in hash
   * @param hashValue Value in hash
   * @return Return value of hash addition operation
   */
  public boolean addToHash(String key, String hashKey, String hashValue) {
    try {
      client.open();
      return client.addToHash(key, hashKey, hashValue);
    } finally {
      client.close();
    }
  }

  /**
   * Gets a value from a hash.
   * @param key Key to use
   * @param hashKey Hash key to use
   * @return Associated value with hash key
   */
  public String getFromHash(String key, String hashKey) {
    try {
      client.open();
      return client.getFromHash(key, hashKey);
    } finally {
      client.close();
    }
  }

  /**
   * Validates that the connection is working.
   * @return If the connection is working
   */
  public boolean validateConnection() {
    return client.validateConnection();
  }

  /**
   * Shutsdown the caching database connection.
   */
  public void shutdown() {
    client.shutdown();
  }
}
