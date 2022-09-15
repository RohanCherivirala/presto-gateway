package com.lyft.data.query.processor.caching;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

import java.util.Arrays;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.codec.Charsets;

@Slf4j
public class S3Connection extends CachingDatabaseConnection {
  public String bucket;
  public static final String prefix = "presto-gateway/test/";

  AmazonS3 client;

  public S3Connection(QueryProcessorConfiguration configuration) {
    try {
      bucket = configuration.getCachingDatabase().getBucket();

      // Create client connection
      client = AmazonS3ClientBuilder
      .standard()
      .withRegion(Regions.US_EAST_1)
      .build();

      log.info("S3 connection successfully created");
    } catch (Exception e) {
      log.error("Error occured while creating S3 connection", e);
    }
  }

  @Override
  public void open() {
    // Open connection
  }

  @Override
  public void close() {
    // Close connection
  }

  @Override
  public String get(String key) {
    try (S3Object response = client.getObject(bucket, getProperKey(key))) {
      return new String(response.getObjectContent().readAllBytes(), Charsets.UTF_8);
    } catch (Exception e) {
      log.error("Error while getting information- Key: {}", key, e);
    }

    return null;
  }

  @Override
  public String set(String key, String value) {
    try {
      client.putObject(bucket, getProperKey(key), value);
      return "OK";
    } catch (Exception e) {
      log.error("Error while setting information- Key: {}", key, e);
    }

    return "NOT OK";
  }

  @Override
  public String getFromHash(String key, String hashKey) {
    return get(getCombinedHashKey(key, hashKey));
  }

  @Override
  public boolean setInHash(String key, String hashKey, String hashValue) {
    return set(getCombinedHashKey(key, hashKey), hashValue).equals("OK");
  }

  @Override
  public long incrementInHash(String key, String hashKey, int amount) {
    try (S3Object response = client.getObject(bucket, getProperKey(key))) {
      int newValue = Integer.valueOf(new String(
          response.getObjectContent().readAllBytes(), Charsets.UTF_8)) + amount;

      setInHash(getProperKey(key), hashKey, Integer.toString(newValue));
      return newValue;
    } catch (Exception e) {
      log.error("Error while getting information- Key: {}", key, e);
    }

    return 0;
  }

  @Override
  public String getFromList(String key) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long addToList(String key, String value) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void deleteKeys(String... keys) {
    try {
      for (String key : keys) {
        client.deleteObject(bucket, getProperKey(key));
      }
    } catch (Exception e) {
      log.error("Error while deleting information- Keys: {}", Arrays.toString(keys), e);
    }
  }

  @Override
  public boolean validateConnection() {
    return true;
  }

  @Override
  public void shutdown() {
    log.info("Closing S3 client");
    client.shutdown();
  }

  /**
   * Returns the key with the proper prefix.
   * @param key Key to use
   */
  private String getProperKey(String key) {
    return prefix + key;
  }

  /**
   * Returns a key represented the combined info of the hash key.
   * @param key Key to use
   * @param hashKey Hash key to use
   */
  private String getCombinedHashKey(String key, String hashKey) {
    return key + "-" + hashKey;
  }
}
