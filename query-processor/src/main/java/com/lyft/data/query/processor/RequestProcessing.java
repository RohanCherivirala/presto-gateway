package com.lyft.data.query.processor;

import com.google.inject.Inject;
import com.lyft.data.query.processor.config.ClusterRequest;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import lombok.extern.slf4j.Slf4j;

import org.asynchttpclient.ListenableFuture;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.Response;
import org.asynchttpclient.util.HttpConstants;

@Slf4j
public class RequestProcessing {
  @Inject 
  private static ThreadPoolExecutor queue;

  /**
   * Processes a request in the queue and sends the corresponding request.
   * @param request Information about the request
   */
  public static void processRequest(ClusterRequest request) 
      throws InterruptedException, ExecutionException {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      QueryProcessor.getQueue().add(request.getQueryId());
    }

    // Build new GET request
    Request getRequest = new RequestBuilder(HttpConstants.Methods.GET)
        .setUrl(request.getNextUri())
        .build();

    // Send request
    ListenableFuture<Response> future = QueryProcessor.getHttpClient()
        .executeRequest(getRequest);

    log.debug("\n\n\nREACHED REQ SEND\n\n\n");
    queue.shutdown();
    
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
      QueryProcessor.getQueue().add(request.getQueryId());
    }

    log.debug("\n\n\nRESPONSE PROCESSED\n\n\n");
  }
}
