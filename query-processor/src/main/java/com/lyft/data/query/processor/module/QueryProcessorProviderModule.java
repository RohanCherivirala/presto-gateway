package com.lyft.data.query.processor.module;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppModule;
import com.lyft.data.query.processor.caching.CachingDatabaseManager;
import com.lyft.data.query.processor.caching.QueryCachingManager;
import com.lyft.data.query.processor.config.QueryProcessorConfiguration;
import com.lyft.data.query.processor.error.ErrorManager;
import com.lyft.data.query.processor.processing.RequestProcessingManager;

import io.dropwizard.setup.Environment;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Dsl;

/**
 * This class serves as a module of the query-processor that provides the thread
 * pool executor.
 */
public class QueryProcessorProviderModule 
    extends AppModule<QueryProcessorConfiguration, Environment> {
  public static final int QUEUE_SIZE = 20;

  private final ThreadPoolExecutor queue;
  private final AsyncHttpClient httpClient;

  private final CachingDatabaseManager cachingManager;
  private final ErrorManager errorManager;
  private final QueryCachingManager queryCachingManager;
  private final RequestProcessingManager requestProcessingManager;

  public static CachingDatabaseManager staticCachingManager;
  public static RequestProcessingManager staticRequestManager;
  public static QueryCachingManager staticQueryCachingManager;

  public QueryProcessorProviderModule(QueryProcessorConfiguration config, Environment env) {
    super(config, env);

    // Set up http client (TODO: Add more properties)
    AsyncHttpClientConfig clientConfig = new DefaultAsyncHttpClientConfig.Builder()
        .setMaxRequestRetry(3)
        .build();

    httpClient = Dsl.asyncHttpClient(clientConfig);

    // Set up thread pool
    queue = (ThreadPoolExecutor) Executors.newCachedThreadPool();

    // Set up managers
    errorManager = new ErrorManager();
    cachingManager = new CachingDatabaseManager(config);
    queryCachingManager = new QueryCachingManager(cachingManager);
    requestProcessingManager = new RequestProcessingManager(
        queue, queryCachingManager, httpClient, errorManager);

    // Set static variables for other modules
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
   * Returns the error manager.
   * @return Error manager
   */
  @Provides
  @Singleton
  public ErrorManager getErrorManager() {
    return errorManager;
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
