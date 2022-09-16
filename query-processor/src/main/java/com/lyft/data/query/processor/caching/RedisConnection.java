package com.lyft.data.query.processor.caching;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.cluster.api.async.RedisAdvancedClusterAsyncCommands;
import io.lettuce.core.cluster.api.reactive.RedisAdvancedClusterReactiveCommands;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class RedisConnection extends CachingDatabaseConnection {
  // In seconds
  public static final int DEFAULT_EXPIRATION_TIME_SECONDS = 600;

  public static final String OK = "OK";

  private static RedisClusterClient client;
  private StatefulRedisClusterConnection<String, String> statefulConnection;
  private RedisAdvancedClusterReactiveCommands<String, String> reactive;
  private RedisAdvancedClusterAsyncCommands<String, String> asyncCommands;

  public RedisConnection(QueryProcessorConfiguration configuration) {
    try {
      client = RedisClusterClient.create(RedisURI.create(
        configuration.getCachingDatabase().getHost(), 
        configuration.getCachingDatabase().getPort()));

      startup();

      if (!validateConnection()) {
        throw new RedisConnectionException("Unable to connect");
      }

      log.debug("Redis connection successfully created");
    } catch (Exception e) {
      log.error("Error occured while creating RedisConnection", e);
    }
  }

  /**
   * Starts a redis connection.
   */
  public void startup() {
    statefulConnection = client.connect();
    reactive = statefulConnection.reactive();
    asyncCommands = statefulConnection.async();
  }

  /**
   * Open a redis connection (Not required for redis as connections are long-lived).
   */
  @Override
  public void open() {
    // Open connection
  }

  /**
   * Close a redis connection (Not required for redis as connections are long-lived).
   */
  @Override
  public void close() {
    // Close connection
  }

  /**
   * Gets the value associated with a key.
   * @param key Redis key
   * @Return The value associated with the key
   */
  @Override
  public String get(String key) {
    Mono<String> response = reactive.get(key);
    return response.block();
  }

  /**
   * Sets a key associated a certain value with a default expiration time.
   * @param key Redis key
   * @param value Value associated with Redis key
   * @return Set response
   */
  @Override
  public String set(String key, String value) {
    return set(key, value, DEFAULT_EXPIRATION_TIME_SECONDS);
  }

  /**
   * Sets a key associated a certain value with a set expiration time.
   * @param key Redis key
   * @param value Value associated with Redis key
   * @param expTime Expiration time (In seconds)
   * @return Set response
   */
  public String set(String key, String value, int expTime) {
    SetArgs setArgs = new SetArgs();
    setArgs.ex(expTime);

    Mono<String> response = reactive.set(key, value, setArgs);
    return response.block();
  }

  /**
   * Gets a value from a redis hash.
   * @param key Key of hash
   * @param hashKey Key to use in hash
   * @return Value associated with key in hash
   */
  public String getFromHash(String key, String hashKey) {
    Mono<String> response = reactive.hget(key, hashKey);
    return response.block();
  }

  /**
   * Adds a key-value mapping to a redis hash.
   * @param key Redis key
   * @param hashKey Key to use in hash
   * @param hashValue Value to use in redis hash
   * @return Boolean response of hset function
   */
  public boolean setInHash(String key, String hashKey, String hashValue) {
    Boolean response = reactive.hset(key, hashKey, hashValue).block();
    reactive.expire(key, DEFAULT_EXPIRATION_TIME_SECONDS).block();
    return response.booleanValue();
  }

  /**
   * Increments a value stored within a redis hash.
   * @param key Key of hash
   * @param hashKey Key to use in hash
   * @param amount Amount to increment by
   * @return New value of field
   */
  public long incrementInHash(String key, String hashKey, int amount) {
    Mono<Long> response = reactive.hincrby(key, hashKey, amount);
    return response.block().longValue();
  }

  /**
   * Gets an element from a redis list if it exists.
   * @param key Redis key
   * @returm Value from list, if one exists
   */
  public String getFromList(String key) {
    Mono<String> response = reactive.lpop(key);
    return response != null ? response.block() : null;
  }

  /**
   * Adds an element to a redis list.
   * @param key Redis key
   * @param value Value to add to list
   * @return Response from redis
   */
  public long addToList(String key, String value) {
    Long response = reactive.lpush(key, value).block();
    reactive.expire(key, DEFAULT_EXPIRATION_TIME_SECONDS).block();
    return response.longValue();
  }

  /**
   * Deletes a set of keys from redis.
   * @param keys Keys to delete
   * @return Number of keys deleted
   */
  public void deleteKeys(String... keys) {
    asyncCommands.del(keys);
  }

  /**
   * Validates that the redis servre is connected.
   * @return If the connection is valid
   */
  @Override
  public boolean validateConnection() {
    Mono<String> response = reactive.ping();
    return response.block().equalsIgnoreCase("pong");
  }
 
  /**
   * Shutsdown the redis client.
   */
  @Override
  public void shutdown() {
    statefulConnection.close();
    client.shutdown();
    log.debug("Redis connection stopped");
  }
}
