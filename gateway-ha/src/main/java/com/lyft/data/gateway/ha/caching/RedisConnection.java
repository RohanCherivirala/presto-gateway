package com.lyft.data.gateway.ha.caching;

import com.lyft.data.gateway.ha.config.HaGatewayConfiguration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class RedisConnection extends CachingDatabaseConnection {
  public static final String DEFAULT_REDIS_STRING = "";
  private static RedisClient client;

  public RedisConnection(HaGatewayConfiguration configuration) {
    try {
      client = RedisClient.create(RedisURI.create(
          configuration.getCachingDatabase().getHost(),
          configuration.getCachingDatabase().getPort()));
      log.debug("Redis connection successfully created");
    } catch (Exception e) {
      log.error("Error occured while creating RedisConnection", e);
    }
  }

  public RedisConnection() {
    this(DEFAULT_REDIS_STRING);
  }

  public RedisConnection(String redisString) {
    client = RedisClient.create(redisString);
  }

  @Override
  public String get(String key) {
    StatefulRedisConnection<String, String> connection = null;

    try {
      connection = client.connect();
      RedisStringReactiveCommands<String, String> reactive = connection.reactive();

      Mono<String> response = reactive.get(key);
      return response.block();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Override
  public void set(String key, String value) {
    StatefulRedisConnection<String, String> connection = null;

    try {
      connection = client.connect();
      RedisStringReactiveCommands<String, String> reactive = connection.reactive();

      Mono<String> response = reactive.set(key, value);
      response.block();
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }

  @Override
  public void testConnection() {
    log.debug("\n\nResponse from redis: {}\n\n", get("testKey"));
  }
 
  @Override
  public void shutdown() {
    client.shutdown();
    log.debug("Redis connection stopped");
  }
}
