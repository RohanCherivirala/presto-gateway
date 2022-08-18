package com.lyft.data.query.processor.queue;

import java.util.concurrent.ThreadFactory;

public class QueryThreadFactory implements ThreadFactory {
  @Override
  public Thread newThread(Runnable r) {
    return new Thread(new QueryThread(r));
  }
}