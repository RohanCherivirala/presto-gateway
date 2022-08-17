package com.lyft.data.query.processor;

import com.lyft.data.query.processor.config.ClusterRequest;

public class RequestProcessing {
  public void processRequest(ClusterRequest request) {
    // App is about to be shutdown
    if (QueryProcessor.isTerminated()) {
      QueryProcessor.queue.add(request);
    }

    
  }
}
