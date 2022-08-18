package com.lyft.data.query.processor.module;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppModule;
import com.lyft.data.query.processor.config.QueryProcessorConfiguration;

import io.dropwizard.setup.Environment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import lombok.extern.slf4j.Slf4j;

/**
 * This class serves as a module of the query-processor that provides the thread
 * pool executor.
 */
@Slf4j
public class ThreadPoolProviderModule extends AppModule<QueryProcessorConfiguration, Environment> {
  public static final int QUEUE_SIZE = 10;

  private ThreadPoolExecutor queue;
  private ExecutorService queueService;

  public ThreadPoolProviderModule(QueryProcessorConfiguration config, Environment env) {
    super(config, env);
    queue = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    queueService = MoreExecutors.getExitingExecutorService(queue);
  }

  /**
   * Provides the thread pool execution queue.
   * @return The thread pool
   */
  @Provides
  @Singleton
  public ThreadPoolExecutor provideQueue() {
    return queue;
  }

  /**
   * Provides executor service for queue.
   * @return Thread pool executor service
   */
  @Provides
  @Singleton
  public ExecutorService provideQueueService() {
    return queueService;
  }
}
