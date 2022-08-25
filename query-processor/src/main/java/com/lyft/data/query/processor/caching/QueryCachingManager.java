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

import org.asynchttpclient.Response;

@Slf4j
public class QueryCachingManager {
  private final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  public static final String COMPLETED = "completed";
  public static final String INITIAL_QUERY_ID = "initialQueryId";
  public static final String RETRIES = "retries";
  
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

      cacheHeader(headerMap, cacheKey, "");

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
      // Indicate that the query has been started
      cacheInActiveQueries(queryId, queryId, 0);

      // Cache request headers
      Enumeration<String> requestHeaders = request.getHeaderNames();
      HashMap<String, String> requestHeaderMap = new HashMap<>();
      while (requestHeaders.hasMoreElements()) {
        String newHeader = requestHeaders.nextElement();
        requestHeaderMap.put(newHeader, request.getHeader(newHeader));
      }

      cacheHeader(requestHeaderMap, 
          CachingDatabaseManager.QUERY_CACHE_PREFIX + queryId,
          CachingDatabaseManager.INITIAL_REQUEST_SUFFIX);

      // Cache request body
      cachingManager.setInHash(CachingDatabaseManager.QUERY_CACHE_PREFIX 
          + queryId + CachingDatabaseManager.INITIAL_REQUEST_SUFFIX, 
          CachingDatabaseManager.BODY_FIELD, requestBody);

      // Cache response headers
      Collection<String> responseHeaders = response.getHeaderNames();
      HashMap<String, String> responseHeaderMap = new HashMap<>();
      for (String newHeader : responseHeaders) {
        responseHeaderMap.put(newHeader, response.getHeader(newHeader));
      }

      // Remove content encoding header if present (Indicate plaintext response)
      responseHeaderMap.remove(HttpHeaders.CONTENT_ENCODING);

      cacheHeader(responseHeaderMap, 
          CachingDatabaseManager.QUERY_CACHE_PREFIX + queryId,
          CachingDatabaseManager.INITIAL_RESPONSE_SUFFIX);

      // Cache response body
      cachingManager.setInHash(CachingDatabaseManager.QUERY_CACHE_PREFIX 
          + queryId + CachingDatabaseManager.INITIAL_RESPONSE_SUFFIX, 
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
  private void cacheHeader(Map<String, String> headers, String prefix, String suffix)
      throws JsonProcessingException {
    String headerString = OBJECT_MAPPER.writeValueAsString(headers);
    cachingManager.setInHash(prefix + suffix, CachingDatabaseManager.HEADER_FIELD, headerString);
  }

  /**
   * Caches the query information in the active queries table.
   * @param queryId QueryId of request
   * @param active 
   * @param retries
   */
  private void cacheInActiveQueries(String queryId, String initialQueryId, int retries) {
    String cacheKey = CachingDatabaseManager.ACTIVE_QUERIES_PREFIX + queryId;
    cachingManager.setInHash(cacheKey, COMPLETED, Boolean.toString(false));
    cachingManager.setInHash(cacheKey, INITIAL_QUERY_ID, initialQueryId);
    cachingManager.setInHash(cacheKey, RETRIES, Integer.toString(retries));
  }

  /**
   * Caches the information that a request has been completed.
   * @param queryId The queryId of the request
   */
  public void cacheRequestCompleted(String queryId) {
    cachingManager.setInHash(CachingDatabaseManager.ACTIVE_QUERIES_PREFIX + queryId,
        COMPLETED, Boolean.toString(true));
  }

  /**
   * Return if the corresponding queryId can be retried.
   * @param queryId Query Id of query to possibly retry
   * @return If the query can be retried
   */
  public boolean canRetry(String queryId) {
    int retriesCompleted = Integer.valueOf(cachingManager.getFromHash(
        CachingDatabaseManager.ACTIVE_QUERIES_PREFIX + queryId, 
        RETRIES));

    return retriesCompleted < QueryProcessor.MAX_RETRIES;
  }

  /**
   * Fills the HttpServlet response based on data in the cache.
   * @param req Http request
   * @param resp Http response
   * @param queryId Query Id
   */
  public void fillResponseForClient(HttpServletRequest req,
      HttpServletResponse resp, String queryId) throws IOException {
    String responseBody = "";
    String activeQueriesKey = CachingDatabaseManager.ACTIVE_QUERIES_PREFIX + queryId;

    if (cachingManager.getFromHash(activeQueriesKey,
        COMPLETED).equals(Boolean.toString(true))) {
      // Complete response has been recieved
      String correctedUri = BaseHandler.removeClientFromUri(req.getRequestURL().toString());
      String cacheKey = getIncrementalCacheKey(correctedUri);
      
      responseBody = cachingManager.getFromHash(cacheKey, CachingDatabaseManager.BODY_FIELD);
    } else {
      // Query is still being processed
      String initialQueryId = cachingManager.getFromHash(activeQueriesKey, INITIAL_QUERY_ID);
      responseBody = cachingManager.getFromHash(CachingDatabaseManager.QUERY_CACHE_PREFIX 
        + initialQueryId + CachingDatabaseManager.INITIAL_RESPONSE_SUFFIX,
        CachingDatabaseManager.BODY_FIELD);
    }

    if (Strings.isNullOrEmpty(responseBody)) {
      throw new InvalidCacheLoadException("No matching entry in cache");
    }

    fillResponseBody(responseBody.getBytes(Charset.defaultCharset()), resp);
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
}
