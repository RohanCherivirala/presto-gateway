package com.lyft.data.server.modules;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppConfiguration;
import com.lyft.data.baseapp.AppModule;

import io.dropwizard.setup.Environment;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ServerProviderModule 
    extends AppModule<AppConfiguration, Environment> {
  private static int SCHEDULED_POOL_SIZE = 25;

  private final ScheduledThreadPoolExecutor scheduledThreadPool;

  public ServerProviderModule(AppConfiguration config, Environment env) {
    super(config, env);

    // Set it up to work as cached thread pool
    scheduledThreadPool = new ScheduledThreadPoolExecutor(SCHEDULED_POOL_SIZE);
    scheduledThreadPool.setKeepAliveTime(10, TimeUnit.SECONDS);
    scheduledThreadPool.allowCoreThreadTimeOut(true);
  }
  
  /**
   * Provides a scheduled thread pool for the application to use.
   * @return Scheduled thread pool
   */
  @Provides
  @Singleton
  public ScheduledThreadPoolExecutor providScheduledThreadPool() {
    return scheduledThreadPool;
  }
}
