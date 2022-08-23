package com.lyft.data.query.processor.caching;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.io.IOException;
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
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
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

    try {
      // Cache response headers
      Iterator<Entry<String, String>> iter = response.getHeaders().iteratorAsString();
      Map<String, String> headerMap = new HashMap<>();
      while (iter.hasNext()) {
        Entry<String, String> next = iter.next();
        headerMap.put(next.getKey(), next.getValue());
      }

      cacheHeader(headerMap, 
          request.getOriginalNextUri(), 
          CachingDatabaseManager.CACHED_PIECE_HEADER_SUFFIX);

      // Cache response body
      cachingManager.set(
          request.getOriginalNextUri() + CachingDatabaseManager.CACHED_PIECE_BODY_SUFFIX,
          responseString);
    } catch (Exception e) {
      log.error("Error caching data for queryId [{}]", request.getQueryId(), e);
    }
  }

  /**
   * Cache the initial response receieved from the presto cluster.
   * 
   * @param request  Initial http servlet request
   * @param response Response to initial servlet request
   */
  public void cacheInitialInformation(HttpServletRequest request,
      HttpServletResponse response, String queryId, String requestBody, String responseBody) {
    try {
      // Indicate that the query has been started
      cachingManager.set(queryId + CachingDatabaseManager.COMPLETION_SUFFIX, "false");

      // Cache request headers
      Enumeration<String> requestHeaders = request.getHeaderNames();
      HashMap<String, String> requestHeaderMap = new HashMap<>();
      while (requestHeaders.hasMoreElements()) {
        String newHeader = requestHeaders.nextElement();
        requestHeaderMap.put(newHeader, request.getHeader(newHeader));
      }

      cacheHeader(requestHeaderMap, queryId, CachingDatabaseManager.INITIAL_REQUEST_HEADERS);

      // Cache request body
      cachingManager.set(queryId + CachingDatabaseManager.INITIAL_REQUEST_BODY, requestBody);

      // Cache response headers
      Collection<String> responseHeaders = response.getHeaderNames();
      HashMap<String, String> responseHeaderMap = new HashMap<>();
      for (String newHeader : responseHeaders) {
        responseHeaderMap.put(newHeader, response.getHeader(newHeader));
      }

      // Remove content encoding header if present (Indicate plaintext response)
      responseHeaderMap.remove(HttpHeaders.CONTENT_ENCODING);

      cacheHeader(responseHeaderMap, queryId, CachingDatabaseManager.INITIAL_RESPONSE_HEADER);

      // Cache response body
      cachingManager.set(queryId + CachingDatabaseManager.INITIAL_RESPONSE_BODY, responseBody);
    } catch (Exception e) {
      log.error("Error caching initial request and response for queryId [{}]", queryId, e);
    }
  }

  /**
   * Cache a map containing header keys and values to a specific key.
   * 
   * @param headers Map of headrs
   * @param prefix  Prefix of key
   * @param suffix  Suffix of key
   */
  private void cacheHeader(Map<String, String> headers, String prefix, String suffix)
      throws JsonProcessingException {
    String headerString = OBJECT_MAPPER.writeValueAsString(headers);
    cachingManager.set(prefix + suffix, headerString);
  }

  /**
   * Caches the information that a request has been completed.
   * @param queryId The queryId of the request
   */
  public void cacheRequestCompleted(String queryId) {
    cachingManager.set(queryId + CachingDatabaseManager.COMPLETION_SUFFIX, "true");
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

    if (cachingManager.get(queryId + CachingDatabaseManager.COMPLETION_SUFFIX).equals("true")) {
      // Complete response has been recieved
      String correctedUrl = BaseHandler.removeClientFromUri(req.getRequestURL().toString());
      
      responseBody = cachingManager.get(correctedUrl
          + CachingDatabaseManager.CACHED_PIECE_BODY_SUFFIX);
    } else {
      // Query is still being processed
      responseBody = cachingManager.get(queryId 
        + CachingDatabaseManager.INITIAL_RESPONSE_BODY);
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
}