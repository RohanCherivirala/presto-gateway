package com.lyft.data.query.processor.module;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppModule;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;
import com.lyft.data.query.processor.caching.QueryCachingManager;
import com.lyft.data.query.processor.config.QueryProcessorConfiguration;
import com.lyft.data.query.processor.processing.RequestProcessingManager;

import io.dropwizard.setup.Environment;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Dsl;

/**
 * This class serves as a module of the query-processor that provides the thread
 * pool executor.
 */
public class QueryProcessorProviderModule 
    extends AppModule<QueryProcessorConfiguration, Environment> {
  public static final int QUEUE_SIZE = 10;

  private final ThreadPoolExecutor queue;
  private final ExecutorService queueService;
  private final CachingDatabaseManager cachingManager;
  private final QueryCachingManager queryCachingManager;
  private final RequestProcessingManager requestProcessingManager;

  private final AsyncHttpClient httpClient;

  public static CachingDatabaseManager staticCachingManager;
  public static RequestProcessingManager staticRequestManager;
  public static QueryCachingManager staticQueryCachingManager;

  public QueryProcessorProviderModule(QueryProcessorConfiguration config, Environment env) {
    super(config, env);

    // Set up http client
    httpClient = Dsl.asyncHttpClient();

    // Set up thread pool
    queue = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    queueService = MoreExecutors.getExitingExecutorService(queue);

    // Set up caching connection
    cachingManager = new CachingDatabaseManager(config);
    queryCachingManager = new QueryCachingManager(cachingManager);
    requestProcessingManager = new RequestProcessingManager(
        queue, queryCachingManager, httpClient);

    QueryProcessorProviderModule.staticCachingManager = cachingManager;
    QueryProcessorProviderModule.staticRequestManager = requestProcessingManager;
    QueryProcessorProviderModule.staticQueryCachingManager = queryCachingManager;
  }

  @Provides
  @Singleton
  public AsyncHttpClient provideHttpClient() {
    return httpClient;
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

  /**
   * Returns the caching database manager.
   * @return Caching database manager
   */
  @Provides
  @Singleton
  public CachingDatabaseManager getCachingDatabaseManager() {
    return cachingManager;
  }

  /**
   * Returns the query caching manager.
   * @return Query caching manager
   */
  @Provides
  @Singleton
  public QueryCachingManager getQueryCachingManager() {
    return queryCachingManager;
  }

  /**
   * Returns the request processing manager.
   * @return Request processing manager
   */
  @Provides
  @Singleton
  public RequestProcessingManager getRequestProcessingManager() {
    return requestProcessingManager;
  }
}
