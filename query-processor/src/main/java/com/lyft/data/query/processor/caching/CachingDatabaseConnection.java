package com.lyft.data.query.processor.caching;

public abstract class CachingDatabaseConnection {
  /**
   * Opens a connection to the database.
   */
  public abstract void open();

  /**
   * Closes a connection to the database.
   */
  public abstract void close();

  /**
   * Gets the information associated with a key.
   * @param key Key to get information from
   * @return Value associated with key
   */
  public abstract String get(String key);

  /**
   * Set key to a value.
   * @param key Key to set
   * @param value Value to set
   * @return Response of set operation
   */
  public abstract String set(String key, String value);

  /**
   * Gets a value from a hash.
   * @param key Ket to use
   * @param hashKey Hash key to use
   * @return Value associated with the hash
   */
  public abstract String getFromHash(String key, String hashKey);

  /**
   * Set a value in the hash.
   * @param key Key to use
   * @param hashKey Hash key to use
   * @param hashValue Value to set
   * @return Boolean return value
   */
  public abstract boolean setInHash(String key, String hashKey, String hashValue);

  /**
   * Increment a value in the hash.
   * @param key Key to use
   * @param hashKey Key of has
   * @param amount Amount to increment by
   * @return New value
   */
  public abstract long incrementInHash(String key, String hashKey, int amount);

  /**
   * Get a value from a list.
   * @param key Key to get
   * @return Value associated with it
   */
  public abstract String getFromList(String key);

  /**
   * Add item to a list.
   * @param key Key to use
   * @param value Value to add
   * @return Length of list
   */
  public abstract long addToList(String key, String value);

  /**
   * Delete a set of keys.
   * @param keys Keys to delete
   */
  public abstract void deleteKeys(String... keys);

  /**
   * Validate that a connection has been made.
   * @return If the connection works
   */
  public abstract boolean validateConnection();

  /**
   * Shutdwon the connection.
   */
  public abstract void shutdown();
}
