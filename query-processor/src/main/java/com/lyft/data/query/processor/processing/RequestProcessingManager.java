package com.lyft.data.query.processor.processing;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.query.processor.QueryProcessor;
import com.lyft.data.query.processor.caching.QueryCachingManager;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.net.URI;
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
import org.eclipse.jetty.http.HttpHeader;

@Slf4j
public class RequestProcessingManager {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final ThreadPoolExecutor queue;
  private final QueryCachingManager queryCachingManager;

  public static String VIA_HOST = "request_processor";
  
  public RequestProcessingManager(ThreadPoolExecutor queue,
      QueryCachingManager queryCachingManager) {
    this.queue = queue;
    this.queryCachingManager = queryCachingManager;
  }

  /**
   * Processes a request in the queue and sends the corresponding request.
   * @param request Information about the request
   */
  public void processRequest(ClusterRequest request) 
      throws InterruptedException, ExecutionException {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      QueryProcessor.getQueue().add(request.getQueryId());
      return;
    }

    log.debug("Sending GET request to [{}]", request.getNextUri());

    // Build new GET request with proxy headers
    Request getRequest = new RequestBuilder(HttpConstants.Methods.GET)
        .setUrl(request.getNextUri())
        .addHeader(HttpHeader.HOST.asString(), request.getHost())
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
  public void processResponse(ClusterRequest request, Response response) {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      return;
    } else {
      QueryProcessor.getQueue().remove(request.getQueryId());
    }

    byte[] body = response.getResponseBodyAsBytes();
    String output = "";

    // Cache information
    queryCachingManager.cacheIncrementalResponse(request, response);

    // Parse response to check for error
    try {
      if (BaseHandler.isGZipEncoding(response.getHeader(HttpHeaders.CONTENT_ENCODING))) {
        output = BaseHandler.plainTextFromGz(body);
      } else {
        output = new String(body);
      }

      JsonNode root = OBJECT_MAPPER.readTree(output);

      // Error found
      if (!root.at("/error").isMissingNode()) {
        int errorCode = root.at("/error/errorCode").asInt();
        String errorName = root.at("/error/errorName").asText();
        String errorType = root.at("/error/errorType").asText();

        log.debug("\n\nError Details:");
        log.debug(String.format("Error Code: %s ErrorName: %s Error Type: %s\n\n", 
                          errorCode, errorName, errorType));

        requestCompleted(request.getQueryId());
      } else {
        if (root.at("/nextUri").isMissingNode()) {
          // No nextUri field
          requestCompleted(request.getQueryId());
        } else {
          // Send next get request if required
          submitNextGetRequest(
              request.getQueryId(),
              root.at("/nextUri").asText(),
              request.getHost(),
              request.getBackendAddress());
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
  }

  public void processNewRequest(HttpServletRequest request,
      HttpServletResponse response, String backendAddress, 
      String queryId, String requestBody, String responseBody) {
    try {
      // Cache all request information
      queryCachingManager.cacheInitialInformation(request, response, 
          queryId, requestBody, responseBody);

      JsonNode root = OBJECT_MAPPER.readTree(responseBody);

      submitNextGetRequest(
          queryId, 
          root.at("/nextUri").asText(), 
          request.getHeader(HttpHeader.HOST.asString()),
          backendAddress);
    } catch (Exception e) {
      log.error("Error occured while processing a new request with queryId [{}]", queryId, e);
    }
  }

  /**
   * This function is called when a request is completed.
   * @param queryId The query id of the completed request.
   */
  private void requestCompleted(String queryId) {
    log.debug("Query [{}] finished processing", queryId);
    queryCachingManager.cacheRequestCompleted(queryId);
  }

  /**
   * Submits the next get request for processing.
   * @param queryId The query id of the request
   * @param nextUri The next uri of the request
   */
  private void submitNextGetRequest(String queryId, String nextUri, 
      String host, String backendAddress) {
    ClusterRequest newRequest = new ClusterRequest(queryId, 
        rewriteNextUri(nextUri, backendAddress), host, backendAddress);

    queue.submit(() -> {
      try {
        processRequest(newRequest);
      } catch (InterruptedException | ExecutionException e) {
        log.error("Error occured while processing request with queryId [{}]",
            newRequest.getQueryId(), e);
      }
    });
  }

  /**
   * Returns a rewritten next uri to send the request to.
   * @param nextUri Previous nextUri
   * @param backendAddress Backend address of cluster
   * @return New nextUri field
   */
  private String rewriteNextUri(String nextUri, String backendAddress) {
    URI uri = URI.create(nextUri);
    String newNextUri = backendAddress
        + uri.getPath()
        + (Strings.isNullOrEmpty(uri.getQuery()) ? "" : uri.getQuery());

    return newNextUri;
  }
}
