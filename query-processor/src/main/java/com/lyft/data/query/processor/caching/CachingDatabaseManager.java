package com.lyft.data.query.processor.caching;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

/**
 * This class serves to manage a connection with a caching database.
 */
public class CachingDatabaseManager {
  public static final String CACHED_PIECE_HEADER_SUFFIX = "-Cached-Piece-Headers";
  public static final String CACHED_PIECE_BODY_SUFFIX = "-Cached-Piece-Body";
  public static final String COMPLETION_SUFFIX = "-Processing-Completed";

  public static final String INITIAL_REQUEST_HEADERS = "-Initial-Request-Headers";
  public static final String INITIAL_REQUEST_BODY = "-Initial-Request-Body";
  public static final String INITIAL_RESPONSE_HEADER = "-Initial-Response-Headers";
  public static final String INITIAL_RESPONSE_BODY = "-Initial-Response-Body";

  public static final String HUNG_QUERIES_KEY = "Active-Queries/Hung-Queries";

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
