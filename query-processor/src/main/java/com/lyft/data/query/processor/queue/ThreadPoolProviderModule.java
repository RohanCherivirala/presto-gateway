package com.lyft.data.query.processor.queue;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppConfiguration;
import com.lyft.data.baseapp.AppModule;

import io.dropwizard.setup.Environment;

/**
 * This class serves as a module of the query-processor that provides the thread
 * pool executor.
 */
public class ThreadPoolProviderModule extends AppModule<AppConfiguration, Environment> {
  private ThreadPoolExecutor queue;
  private ExecutorService queueService;

  public ThreadPoolProviderModule(AppConfiguration config, Environment env) {
    super(config, env);
    queue = (ThreadPoolExecutor) Executors.newCachedThreadPool();
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
