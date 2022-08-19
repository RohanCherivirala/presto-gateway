package com.lyft.data.query.processor.caching;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

public class CachingDatabaseManager {
  public static final String CACHED_PIECE_HEADER_SUFFIX = "-Cached-Piece-Headers";
  public static final String CACHED_PIECE_BODY_SUFFIX = "-Cached-Piece-Body";
  public static final String COMPLETION_SUFFIX = "-Processing-Completed";

  public static final String INITIAL_REQUEST_HEADERS = "-Initial-Request-Headers";
  public static final String INITIAL_REQUEST_BODY = "-Initial-Request-Body";
  public static final String INITIAL_RESPONSE_HEADER = "-Initial-Response-Headers";
  public static final String INITIAL_RESPONSE_BODY = "-Initial-Response-Body";

  private QueryProcessorConfiguration configuration;
  private CachingDatabaseConnection client;

  public CachingDatabaseManager(QueryProcessorConfiguration configuration) {
    this.configuration = configuration;
  }

  public void start() {
    if (configuration.getCachingDatabase() != null
        && configuration.getCachingDatabase().getDatabaseType().equalsIgnoreCase("redis")) {
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
