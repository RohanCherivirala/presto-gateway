package com.lyft.data.gateway.ha.module;

import com.codahale.metrics.Meter;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.lyft.data.baseapp.AppModule;
import com.lyft.data.gateway.ha.caching.CachingDatabaseManager;
import com.lyft.data.gateway.ha.config.HaGatewayConfiguration;
import com.lyft.data.gateway.ha.config.RequestRouterConfiguration;
import com.lyft.data.gateway.ha.handler.QueryIdCachingServerHandler;
import com.lyft.data.gateway.ha.persistence.JdbcConnectionManager;
import com.lyft.data.gateway.ha.router.GatewayBackendManager;
import com.lyft.data.gateway.ha.router.HaGatewayManager;
import com.lyft.data.gateway.ha.router.HaQueryHistoryManager;
import com.lyft.data.gateway.ha.router.HaRoutingManager;
import com.lyft.data.gateway.ha.router.PrestoQueueLengthRoutingTable;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingManager;
import com.lyft.data.server.GatewayServer;
import com.lyft.data.server.config.GatewayServerConfiguration;
import com.lyft.data.server.handler.ServerHandler;

import io.dropwizard.setup.Environment;

public class HaGatewayProviderModule extends AppModule<HaGatewayConfiguration, Environment> {

  private final GatewayBackendManager gatewayBackendManager;
  private final QueryHistoryManager queryHistoryManager;
  private final RoutingManager routingManager;
  private final JdbcConnectionManager connectionManager;
  private final CachingDatabaseManager cachingManager;

  public HaGatewayProviderModule(HaGatewayConfiguration configuration, Environment environment) {
    super(configuration, environment);
    connectionManager = new JdbcConnectionManager(configuration.getDataStore());
    gatewayBackendManager = new HaGatewayManager(connectionManager);
    queryHistoryManager = new HaQueryHistoryManager(configuration, connectionManager);
    routingManager =
        new PrestoQueueLengthRoutingTable(gatewayBackendManager,
                (HaQueryHistoryManager) queryHistoryManager);
    cachingManager = new CachingDatabaseManager(configuration);
  }

  protected ServerHandler getProxyHandler() {
    Meter requestMeter =
        getEnvironment()
            .metrics()
            .meter(getConfiguration().getRequestRouter().getName() + ".requests");
    return new QueryIdCachingServerHandler(
        getQueryHistoryManager(), getRoutingManager(), getCachingDatabaseManager(),
        getApplicationPort(), requestMeter);
  }

  @Provides
  @Singleton
  public GatewayServer provideGateway() {
    GatewayServer gateway = null;
    if (getConfiguration().getRequestRouter() != null) {
      // Setting up request router
      RequestRouterConfiguration routerConfiguration = getConfiguration().getRequestRouter();

      GatewayServerConfiguration routerProxyConfig = new GatewayServerConfiguration();
      routerProxyConfig.setLocalPort(routerConfiguration.getPort());
      routerProxyConfig.setName(routerConfiguration.getName());
      routerProxyConfig.setProxyTo("");
      routerProxyConfig.setSsl(routerConfiguration.isSsl());
      routerProxyConfig.setKeystorePath(routerConfiguration.getKeystorePath());
      routerProxyConfig.setKeystorePass(routerConfiguration.getKeystorePass());

      ServerHandler proxyHandler = getProxyHandler();
      gateway = new GatewayServer(routerProxyConfig, proxyHandler);
    }
    return gateway;
  }

  @Provides
  @Singleton
  public GatewayBackendManager getGatewayBackendManager() {
    return this.gatewayBackendManager;
  }

  @Provides
  @Singleton
  public QueryHistoryManager getQueryHistoryManager() {
    return this.queryHistoryManager;
  }

  @Provides
  @Singleton
  public RoutingManager getRoutingManager() {
    return this.routingManager;
  }

  @Provides
  @Singleton
  public JdbcConnectionManager getConnectionManager() {
    return this.connectionManager;
  }

  @Provides
  @Singleton
  public CachingDatabaseManager getCachingDatabaseManager() {
    return this.cachingManager;
  }
}