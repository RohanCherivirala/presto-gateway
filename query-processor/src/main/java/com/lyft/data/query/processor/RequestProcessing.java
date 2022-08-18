package com.lyft.data.query.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.HttpHeaders;

import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;

@Slf4j
public class RequestProcessing {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Inject 
  private static ThreadPoolExecutor queue;

  @Inject
  private static CachingDatabaseManager cachingManager;

  /**
   * Processes a request in the queue and sends the corresponding request.
   * @param request Information about the request
   */
  public static void processRequest(ClusterRequest request) 
      throws InterruptedException, ExecutionException {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      QueryProcessor.getQueue().add(request.getQueryId());
      return;
    }

    // Build new GET request
    Request getRequest = new RequestBuilder(HttpConstants.Methods.GET)
        .setUrl(request.getNextUri())
        .build();

    // Send request
    ListenableFuture<Response> future = QueryProcessor.getHttpClient()
        .executeRequest(getRequest);

    // Add request to queue if process terminates
    QueryProcessor.getQueue().add(request.getQueryId());
    
    future.addListener(() -> {
      try {
        processResponse(request, future.get());
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error occured while sending request", e);
      }
    }, queue);
  }

  /**
   * Processes a response received from a cluster.
   * @param response Response recieved
   */
  public static void processResponse(ClusterRequest request, Response response) {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      return;
    } else {
      QueryProcessor.getQueue().remove(request.getQueryId());
    }

    byte[] body = response.getResponseBodyAsBytes();
    String responseString = new String(body);
    String output = "";

    try {
      // Cache response headers
      Iterator<Entry<String, String>> iter = response.getHeaders().iteratorAsString();
      Map<String, String> headerMap = new HashMap<>();
      while (iter.hasNext()) {
        Entry<String, String> next = iter.next();
        headerMap.put(next.getKey(), next.getValue());
      }

      String headerString = OBJECT_MAPPER.writeValueAsString(headerMap);

      cachingManager.set(request.getNextUri() + CachingDatabaseManager.CACHED_PIECE_HEADER_SUFFIX, 
          headerString);

      // Cache response body
      cachingManager.set(request.getNextUri() + CachingDatabaseManager.CACHED_PIECE_BODY_SUFFIX,
          responseString);
    } catch (Exception e) {
      log.error("Error caching data for queryId [{}]", request.getQueryId(), e);
    }

    // Parse response to check for error
    try {
      if (BaseHandler.isGZipEncoding(response.getHeader(HttpHeaders.CONTENT_ENCODING))) {
        output = BaseHandler.plainTextFromGz(body);
      } else {
        output = new String(body);
      }

      JsonNode root = OBJECT_MAPPER.readTree(output);

      // Error found
      if (!root.at("/error").isNull()) {
        int errorCode = root.at("/error/errorCode").asInt();
        String errorName = root.at("/error/errorName").asText();
        String errorType = root.at("/error/errorType").asText();

        log.debug("\n\nError Details:");
        log.debug(String.format("Error Code: %s ErrorName: %s Error Type: %s\n\n", 
                          errorCode, errorName, errorType));
      } else {
        // Send next get request if required
        if (root.at("nextUri").isNull()) {
          // No nextUri field
          requestCompleted(request.getQueryId());
        } else {
          submitNextGetRequest(
             request.getQueryId(), root.at("nextUri").asText());
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
  }

  /**
   * This function is called when a request is completed.
   * @param queryId The query id of the completed request.
   */
  private static void requestCompleted(String queryId) {
    cachingManager.set(queryId + CachingDatabaseManager.COMPLETION_SUFFIX, "true");
  }

  /**
   * Submits the next get request for processing.
   * @param queryId The query id of the request
   * @param nextUri The next uri of the request
   */
  private static void submitNextGetRequest(String queryId, String nextUri) {
    ClusterRequest newRequest = new ClusterRequest(queryId, nextUri);

    queue.submit(() -> {
      try {
        processRequest(newRequest);
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error occured while processing request with queryId [{}]",
            newRequest.getQueryId(), e);
      }
    });
  }
}
