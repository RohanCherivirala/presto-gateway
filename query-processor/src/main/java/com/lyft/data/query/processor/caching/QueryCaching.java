package com.lyft.data.query.processor.caching;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.query.processor.config.ClusterRequest;

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
public class QueryCaching {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  
  @Inject
  private static CachingDatabaseManager cachingManager;

  /**
   * Caches the response.
   * @param request Request information
   * @param response Response information
   */
  public static void cacheIncrementalResponse(ClusterRequest request, Response response) {
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
          request.getNextUri(), 
          CachingDatabaseManager.CACHED_PIECE_HEADER_SUFFIX);

      // Cache response body
      cachingManager.set(request.getNextUri() + CachingDatabaseManager.CACHED_PIECE_BODY_SUFFIX,
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
  public static void cacheInitialInformation(HttpServletRequest request,
      HttpServletResponse response, String queryId, String requestBody, String responseBody) {
    try {
      // Cache request headers
      Enumeration<String> requestHeaders = request.getHeaderNames();
      HashMap<String, String> requestHeaderMap = new HashMap<>();
      while (requestHeaders.hasMoreElements()) {
        String nHeader = requestHeaders.nextElement();
        requestHeaderMap.put(nHeader, request.getHeader(nHeader));
      }

      cacheHeader(requestHeaderMap, queryId, CachingDatabaseManager.INITIAL_REQUEST_HEADERS);

      // Cache request body
      cachingManager.set(queryId + CachingDatabaseManager.INITIAL_REQUEST_BODY, requestBody);

      // Cache response headers
      Collection<String> responseHeaders = response.getHeaderNames();
      HashMap<String, String> responseHeaderMap = new HashMap<>();
      for (String nHeader : responseHeaders) {
        responseHeaderMap.put(nHeader, response.getHeader(nHeader));
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
  private static void cacheHeader(Map<String, String> headers, String prefix, String suffix)
      throws JsonProcessingException {
    String headerString = OBJECT_MAPPER.writeValueAsString(headers);
    cachingManager.set(prefix + suffix, headerString);
  }
}
