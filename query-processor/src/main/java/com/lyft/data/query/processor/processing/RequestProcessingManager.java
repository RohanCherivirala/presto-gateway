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
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;
import org.eclipse.jetty.http.HttpHeader;

/**
 * This class serves to process queries and provides methods to send requests
 * to clusters and parse the response.
 */
@Slf4j
public class RequestProcessingManager {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String NEXT_URI_PATH = "/nextUri";
  public static final String ERROR_PATH = "/error";
  public static final String ERROR_CODE_PATH = "/error/errorCode";
  public static final String ERROR_NAME_PATH = "/error/errorName";
  public static final String ERROR_TYPE_PATH = "/error/errorType";

  private final ThreadPoolExecutor queue;
  private final QueryCachingManager queryCachingManager;
  private final AsyncHttpClient httpClient;
  
  public RequestProcessingManager(ThreadPoolExecutor queue,
      QueryCachingManager queryCachingManager,
      AsyncHttpClient httpClient) {
    this.queue = queue;
    this.queryCachingManager = queryCachingManager;
    this.httpClient = httpClient;
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
    ListenableFuture<Response> future = httpClient.executeRequest(getRequest);
    
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
   * @param request Cluster request that is sent
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
      if (!root.at(ERROR_PATH).isMissingNode()) {
        int errorCode = root.at(ERROR_CODE_PATH).asInt();
        String errorName = root.at(ERROR_NAME_PATH).asText();
        String errorType = root.at(ERROR_TYPE_PATH).asText();

        log.debug("Error Details:");
        log.debug("Error Code: {}, Error Name: {}, Error Type: {}",
            errorCode, errorName, errorType);

        // Attempt to retury query if possible
        if (isRetryNeccessary(errorCode, errorName, errorType)
            && queryCachingManager.canRetry(request.getQueryId())) {
          retryRequest(request.getQueryId());
        } else {
          // Query will not be retried
          requestCompleted(request.getQueryId(), false);
        }
      } else {
        if (root.at(NEXT_URI_PATH).isMissingNode()) {
          // No nextUri field
          requestCompleted(request.getQueryId(), true);
        } else {
          // Send next get request if required
          submitNextGetRequest(
              request.getQueryId(),
              root.at(NEXT_URI_PATH).asText(),
              request.getHost(),
              request.getBackendAddress());
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
  }

  /**
   * Process a new request that is recieved.
   * @param request Http request
   * @param response Http response
   * @param backendAddress Backend address of cluster
   * @param queryId QueryId
   * @param requestBody Body of request
   * @param responseBody Body of response
   */
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

  public void processRetriedRequest(HttpServletRequest request,
      String backendAddress, String queryId, String transactionId, String responseBody) {
    try {
      // Cache that the request has been retried
      queryCachingManager.cacheRetriedRequest(queryId, transactionId);

      JsonNode root = OBJECT_MAPPER.readTree(responseBody);

      submitNextGetRequest(
          queryId, 
          root.at("/nextUri").asText(), 
          request.getHeader(HttpHeader.HOST.asString()),
          backendAddress);
    } catch (Exception e) {
      log.error("Error while retrying request with queryId [{}] and transactionId [{}]",
          queryId, transactionId, e);
    }
  }

  /**
   * This function retries a query without having its previous backend known.
   * This function will mainly be used when dealing with retrying dropped
   * requests.
   * @param queryId
   */
  public void retryRequest(String queryId) {
    retryRequest(queryId, "");
  }

  /**
   * This function takes in a queryId and attempts to retry it.
   * @param queryId QueryId of request to retry
   */
  public void retryRequest(String queryId, String previousAddress) {
    log.debug("Retrying query with query id [{}]", queryId);

    // Build request to send to proxyserver
    RequestBuilder requestBuilder = new RequestBuilder(HttpConstants.Methods.POST)
        .setUrl(BaseHandler.RETRY_PATH)
        .addHeader(BaseHandler.RETRY_BACKEND_EXCLUSION, previousAddress);

    queryCachingManager.fillRetryRequest(queryId, requestBuilder);

    httpClient.executeRequest(requestBuilder.build());
  }

  /**
   * Returns whether or not we should retry the query dependant on the error.
   * Possible error types are external, insufficient resources, internal, or user.
   * @param errorCode Code of error
   * @param errorName Name of error
   * @param errorType Type of error 
   * @return If the query should be retried
   */
  public boolean isRetryNeccessary(int errorCode, String errorName, String errorType) {
    return true;
  }

  /**
   * This function is called when a request is completed.
   * @param queryId The query id of the completed request.
   */
  private void requestCompleted(String queryId, boolean successful) {
    log.debug("Query [{}] finished processing {}", queryId,
        successful ? "succesfully" : "with an error");
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
        rewriteNextUriToBackend(nextUri, backendAddress), host, backendAddress);

    // Check if application is still running
    if (QueryProcessor.isTerminated()) {
      QueryProcessor.getQueue().add(queryId);
      return;
    }

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
  private String rewriteNextUriToBackend(String nextUri, String backendAddress) {
    URI uri = URI.create(nextUri);
    String newNextUri = backendAddress
        + uri.getPath()
        + (Strings.isNullOrEmpty(uri.getQuery()) ? "" : uri.getQuery());

    return newNextUri;
  }
}
