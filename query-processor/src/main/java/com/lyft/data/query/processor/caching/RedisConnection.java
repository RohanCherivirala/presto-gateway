package com.lyft.data.query.processor.caching;

import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;

import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class RedisConnection extends CachingDatabaseConnection {
  public static final String VALIDATE_REDIS_KEY = "validate_redis_key";
  public static final String VALIDATE_RESPONSE = "validated_";

  // In seconds
  public static final int DEFAULT_EXPIRATION_TIME_SECONDS = 600;

  public static final String OK = "OK";

  private static RedisClient client;
  private StatefulRedisConnection<String, String> statefulConnection;
  private RedisReactiveCommands<String, String> reactive;

  public RedisConnection(QueryProcessorConfiguration configuration) {
    try {
      client = RedisClient.create(RedisURI.create(
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
  }

  /**
   * Open a redis connection (Not required for redis as connections are long-lived).
   */
  @Override
  public void open() {
    log.debug("Redis connection opened");
  }

  /**
   * Close a redis connection (Not required for redis as connections are long-lived).
   */
  @Override
  public void close() {
    log.debug("Redis connection closed");
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
   * Adds a key-value mapping to a redis hash.
   * @param key Redis key
   * @param hashKey Key to use in hash
   * @param hashValue Value to use in redis hash
   * @return Boolean response of hset function
   */
  public boolean setInHash(String key, String hashKey, String hashValue) {
    Mono<Boolean> response = reactive.hset(key, hashKey, hashValue);
    reactive.expire(key, DEFAULT_EXPIRATION_TIME_SECONDS);
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
   * Adds an element to a redis list.
   * @param key Redis key
   * @param value Value to add to list
   * @return Response from redis
   */
  public long addToList(String key, String value) {
    Mono<Long> response = reactive.lpush(key, value);
    reactive.expire(key, DEFAULT_EXPIRATION_TIME_SECONDS);
    return response.block().longValue();
  }

  /**
   * Validates that the redis servre is connected.
   * @return If the connection is valid
   */
  @Override
  public boolean validateConnection() {
    Random rand = new Random();
    long randVal = rand.nextLong();

    try {
      open();
      return set(VALIDATE_REDIS_KEY, VALIDATE_RESPONSE + randVal, 20).equals(OK)
              && get(VALIDATE_REDIS_KEY).equals(VALIDATE_RESPONSE + randVal);
    } catch (Exception e) {
      log.error("Error connecting to Redis", e);
      return false;
    } finally {
      close();
    }
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
