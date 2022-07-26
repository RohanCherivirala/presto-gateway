package com.lyft.data.gateway.ha.caching;

import com.lyft.data.gateway.ha.config.HaGatewayConfiguration;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisStringReactiveCommands;

import java.util.Random;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Mono;

@Slf4j
public class RedisConnection extends CachingDatabaseConnection {
  public static final String VALIDATE_REDIS_KEY = "validate_redis_key";
  public static final String VALIDATE_RESPONSE = "validated_";

  // In seconds
  public static final int DEFAULT_EXPIRATION_TIME = 600;

  public static final String OK = "OK";

  private static RedisClient client;
  private StatefulRedisConnection<String, String> statefulConnection;
  private RedisStringReactiveCommands<String, String> reactive;

  public RedisConnection(HaGatewayConfiguration configuration) {
    try {
      client = RedisClient.create(RedisURI.create(
          configuration.getCachingDatabase().getHost(),
          configuration.getCachingDatabase().getPort()));

      if (!validateConnection()) {
        throw new RedisConnectionException("Unable to connect");
      }

      log.debug("Redis connection successfully created");
    } catch (Exception e) {
      log.error("Error occured while creating RedisConnection", e);
    }
  }

  @Override
  public void open() {
    statefulConnection = client.connect();
    reactive = statefulConnection.reactive();
    log.debug("Redis connection opened");
  }

  @Override
  public void close() {
    statefulConnection.close();
    log.debug("Redis connection closed");
  }

  @Override
  public String get(String key) {
    Mono<String> response = reactive.get(key);
    return response.block();
  }

  @Override
  public String set(String key, String value) {
    return set(key, value, DEFAULT_EXPIRATION_TIME);
  }

  public String set(String key, String value, int expTime) {
    SetArgs setArgs = new SetArgs();
    setArgs.ex(expTime);

    Mono<String> response = reactive.set(key, value);
    return response.block();
  }

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
 
  @Override
  public void shutdown() {
    client.shutdown();
    log.debug("Redis connection stopped");
  }
}
