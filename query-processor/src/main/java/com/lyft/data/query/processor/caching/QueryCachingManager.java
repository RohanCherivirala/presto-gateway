package com.lyft.data.query.processor.caching;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.query.processor.QueryProcessor;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.HttpHeaders;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;

@Slf4j
public class QueryCachingManager {
  private final ObjectMapper objectMapper = new ObjectMapper();
  
  public static final String TRANSACTION_ID = "transaction-id";
  public static final String RETRIES = "retries";
  public static final String COMPLETED_ID = "completed-id";
  
  private CachingDatabaseManager cachingManager;

  /**
   * Constructor for a query caching manager.
   * @param cachingManager Caching database manager
   */
  public QueryCachingManager(CachingDatabaseManager cachingManager) {
    this.cachingManager = cachingManager;
  }

  /**
   * Caches the response.
   * @param request Request information
   * @param response Response information
   */
  public void cacheIncrementalResponse(ClusterRequest request, Response response) {
    String responseString = response.getResponseBody();
    String cacheKey = getIncrementalCacheKey(request.getNextUri());

    try {
      // Cache response headers
      Iterator<Entry<String, String>> iter = response.getHeaders().iteratorAsString();
      Map<String, String> headerMap = new HashMap<>();
      while (iter.hasNext()) {
        Entry<String, String> next = iter.next();
        headerMap.put(next.getKey(), next.getValue());
      }

      cacheHeader(headerMap, cacheKey);

      // Cache response body
      cachingManager.setInHash(cacheKey, CachingDatabaseManager.BODY_FIELD,
          responseString);
    } catch (Exception e) {
      log.error("Error caching data for queryId [{}]", request.getQueryId(), e);
    }
  }

  /**
   * Cache the initial response receieved from the presto cluster.
   * @param request  Initial http servlet request
   * @param response Response to initial servlet request
   */
  public void cacheInitialInformation(HttpServletRequest request,
      HttpServletResponse response, String queryId, String requestBody, String responseBody) {
    try {
      // Cache entry in active queries
      String cacheKey = getActiveQueriesKey(queryId);
      cachingManager.setInHash(cacheKey, TRANSACTION_ID, queryId);
      cachingManager.setInHash(cacheKey, RETRIES, "0");

      // Cache request headers
      Enumeration<String> requestHeaders = request.getHeaderNames();
      HashMap<String, String> requestHeaderMap = new HashMap<>();
      while (requestHeaders.hasMoreElements()) {
        String newHeader = requestHeaders.nextElement();
        requestHeaderMap.put(newHeader, request.getHeader(newHeader));
      }

      cacheHeader(requestHeaderMap, getInitialRequestKey(queryId));

      // Cache request body
      cachingManager.setInHash(getInitialRequestKey(queryId), 
          CachingDatabaseManager.BODY_FIELD, requestBody);

      // Cache response headers
      Collection<String> responseHeaders = response.getHeaderNames();
      HashMap<String, String> responseHeaderMap = new HashMap<>();
      for (String newHeader : responseHeaders) {
        responseHeaderMap.put(newHeader, response.getHeader(newHeader));
      }

      // Remove content encoding header if present (Indicate plaintext response)
      responseHeaderMap.remove(HttpHeaders.CONTENT_ENCODING);

      cacheHeader(responseHeaderMap, getInitalResponseKey(queryId));

      // Cache response body
      cachingManager.setInHash(getInitalResponseKey(queryId), 
          CachingDatabaseManager.BODY_FIELD, responseBody);
    } catch (Exception e) {
      log.error("Error caching initial request and response for queryId [{}]", queryId, e);
    }
  }

  /**
   * Cache a map containing header keys and values to a specific key.
   * @param headers Map of headrs
   * @param prefix  Prefix of key
   * @param suffix  Suffix of key
   */
  private void cacheHeader(Map<String, String> headers, String key)
      throws JsonProcessingException {
    String headerString = objectMapper.writeValueAsString(headers);
    cachingManager.setInHash(key, CachingDatabaseManager.HEADER_FIELD, headerString);
  }

  /**
   * Caches the information that a request has been completed.
   * @param queryId The queryId of the request
   */
  public void cacheRequestCompleted(String queryId) {
    cachingManager.setInHash(getActiveQueriesKey(getTransactionId(queryId)),
        COMPLETED_ID, queryId);
  }

  /**
   * Return if the corresponding queryId can be retried.
   * @param queryId Query Id of query to possibly retry
   * @return If the query can be retried
   */
  public boolean canRetry(String queryId) {
    int retriesCompleted = Integer.valueOf(cachingManager.getFromHash(
        getActiveQueriesKey(getTransactionId(queryId)), 
        RETRIES));

    return retriesCompleted < QueryProcessor.MAX_RETRIES;
  }

  /**
   * Update cache to indicate that a query has been retried.
   * @param queryId Query id being retried
   */
  public void cacheRetriedRequest(String queryId, String transactionId) {
    cacheRetriedRequest(queryId, transactionId, 1);
  }

  /**
   * Update cache to indicate that a query has been retried and specify the amount
   * to increment the retry count by.
   * @param queryId Query id being retried
   * @param transactionId Transaction id of retried query
   * @param incrementAmount Amount to increment by
   */
  public void cacheRetriedRequest(String queryId, String transactionId, int incrementAmount) {
    // Create new entry for retry
    cachingManager.setInHash(getActiveQueriesKey(queryId), 
        TRANSACTION_ID, transactionId);

    cachingManager.incrementInHash(getActiveQueriesKey(transactionId), 
        RETRIES, incrementAmount);
  }

  /**
   * Fills a request builder with information from the cache.
   * @param builder Request builder to fill with information
   */
  public void fillRetryRequest(String queryId, RequestBuilder builder) {
    String transactionId = getTransactionId(queryId);

    // Add transactionId header
    builder.addHeader(BaseHandler.RETRY_TRANSACTION_ID, transactionId);

    // Add request headers
    HashMap<String, String> headers = getHeadersFromCache(getInitialRequestKey(transactionId));

    for (Entry<String, String> header : headers.entrySet()) {
      if (header.getKey().toLowerCase().contains(BaseHandler.PRESTO)
          || header.getKey().toLowerCase().contains(BaseHandler.TRINO)) {
        builder.addHeader(header.getKey(), header.getValue());
      }
    }

    // Add request body
    builder.setBody(cachingManager.getFromHash(getInitialRequestKey(transactionId),
        CachingDatabaseManager.BODY_FIELD));
  }

  /**
   * Fills the HttpServlet response based on data in the cache.
   * @param req Http request
   * @param resp Http response
   * @param queryId Query Id
   */
  public void fillResponseForClient(HttpServletRequest req,
      HttpServletResponse resp, String queryId, Boolean completed) throws IOException {
    String responseBody = "";
    String transactionId = getTransactionId(queryId);
    String completedId = cachingManager.getFromHash(getActiveQueriesKey(transactionId), 
        COMPLETED_ID);

    if (!Strings.isNullOrEmpty(completedId)) {
      // Query is completed and complete response has been received
      completed = Boolean.TRUE;

      String correctedUri = BaseHandler.removeClientFromUri(req.getRequestURL().toString())
                                       .replace(transactionId, completedId);
      String cacheKey = getIncrementalCacheKey(correctedUri);
      
      fillResponseHeader(getHeadersFromCache(cacheKey), resp);
      responseBody = cachingManager.getFromHash(cacheKey, CachingDatabaseManager.BODY_FIELD);

      // Delete key from cache
      cachingManager.deleteKeys(cacheKey);
    } else {
      // Query is still being processed
      completed = Boolean.FALSE;
      
      responseBody = cachingManager.getFromHash(
        getInitalResponseKey(transactionId),
        CachingDatabaseManager.BODY_FIELD);
    }

    if (Strings.isNullOrEmpty(responseBody)) {
      throw new InvalidCacheLoadException("No matching entry in cache for transactionId"
          + transactionId);
    }

    fillResponseBody(responseBody.getBytes(Charset.defaultCharset()), resp);
  }

  /**
   * Fills response header based on header map.
   * @param headers Map of headers
   * @param response Reponse to be sent to the client
   */
  private void fillResponseHeader(Map<String, String> headers,
      HttpServletResponse response) {
    for (Entry<String, String> header : headers.entrySet()) {
      response.addHeader(header.getKey(), header.getValue());
    }
  }

  /**
   * Fills response body based on byte array.
   * @param bytes Bytes to fill response body
   * @param response Response to be sent to client
   * @throws IOException
   */
  private void fillResponseBody(final byte[] bytes, HttpServletResponse response) 
      throws IOException {
    response.setHeader(BaseHandler.CONTENT_LENGTH_HEADER, bytes.length + "");
    response.setHeader(BaseHandler.CONTENT_TYPE_HEADER, "application/json");
    response.setDateHeader(BaseHandler.DATE_HEADER, System.currentTimeMillis());

    response.getOutputStream().write(bytes);
  }

  /**
   * Edits the cached response when an error is recieved.
   * @param newBody The new body to be stored
   */
  public void editCacheForError(ClusterRequest request, String newBody)
      throws JsonProcessingException {
    String cacheKey = getIncrementalCacheKey(request.getNextUri());

    // Remove content encoding header
    Map<String, String> headers = getHeadersFromCache(cacheKey);
    headers.remove(HttpHeaders.CONTENT_ENCODING);

    cacheHeader(headers, cacheKey);

    // Set request body
    cachingManager.setInHash(cacheKey, CachingDatabaseManager.BODY_FIELD, newBody);
  }


  /**
   * Gets the headers associated with the specific key and returns it as a hash map.
   * @param key Key to find headers for
   * @return HashMap containing headers of the key
   */
  private HashMap<String, String> getHeadersFromCache(String key) {
    try {
      String headerString = cachingManager.getFromHash(key, CachingDatabaseManager.HEADER_FIELD);
      return objectMapper.readValue(headerString, HashMap.class);
    } catch (IOException e) {
      log.debug("Unable to fetch and read headers for key [{}]", key);
    }

    return null;
  }

  /**
   * Returns the transaction id of the request (which is the initial query id).
   * @param queryId Current query id
   * @return Initial query id
   */
  private String getTransactionId(String queryId) {
    return cachingManager.getFromHash(getActiveQueriesKey(queryId), TRANSACTION_ID);
  }

  /**
   * Returns a key to use in the caching database that corresponds to
   * the given nextUri.
   * @param nextUri Next uri of request
   * @return Key to use
   */
  private String getIncrementalCacheKey(String nextUri) {
    URI uri = URI.create(nextUri);
    return CachingDatabaseManager.QUERY_CACHE_PREFIX.replace("/", "")
        + uri.getPath() + CachingDatabaseManager.CACHED_RESONSE_SUFFIX;
  }

  /**
   * Returns the key to access the initial request for a query id.
   * @param queryId Query id of key
   * @return Initial request key
   */
  private String getInitialRequestKey(String queryId) {
    return  CachingDatabaseManager.QUERY_CACHE_PREFIX + queryId 
        + CachingDatabaseManager.INITIAL_REQUEST_SUFFIX;
  }

  /**
   * Returns the key to access the initial response for a query id.
   * @param queryId Query id of key
   * @return Initial response key
   */
  private String getInitalResponseKey(String queryId) {
    return CachingDatabaseManager.QUERY_CACHE_PREFIX + queryId
        + CachingDatabaseManager.INITIAL_RESPONSE_SUFFIX;
  }

  /**
   * Returns the key to access the active query information for a query id.
   * @param queryId Query id of key
   * @return Active queries key
   */
  private String getActiveQueriesKey(String queryId) {
    return CachingDatabaseManager.ACTIVE_QUERIES_PREFIX + queryId;
  }
}
