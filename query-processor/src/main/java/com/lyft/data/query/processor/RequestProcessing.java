package com.lyft.data.query.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;
import com.lyft.data.query.processor.caching.QueryCaching;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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

    // Add request to queue if process terminates
    QueryProcessor.getQueue().add(request.getQueryId());

    // Send request
    ListenableFuture<Response> future = QueryProcessor.getHttpClient()
        .executeRequest(getRequest);
    
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
    String output = "";

    // Cache information
    QueryCaching.cacheIncrementalResponse(request, response);

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

        requestCompleted(request.getQueryId());
      } else {
        if (root.at("nextUri").isNull()) {
          // No nextUri field
          requestCompleted(request.getQueryId());
        } else {
          // Send next get request if required
          submitNextGetRequest(
              request.getQueryId(), root.at("nextUri").asText());
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
  }

  public static void processNewRequest(HttpServletRequest request,
      HttpServletResponse response, String queryId, String requestBody, String responseBody) {
    try {
      // Cache all request information
      QueryCaching.cacheInitialInformation(request, response, queryId, requestBody, responseBody);

      JsonNode root = OBJECT_MAPPER.readTree(responseBody);
      submitNextGetRequest(
          queryId, root.at("nextUri").asText());
    } catch (Exception e) {
      log.error("Error occured while processing a new request with queryId [{}]", queryId, e);
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
