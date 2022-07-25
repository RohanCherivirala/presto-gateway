package com.lyft.data.server;

import com.lyft.data.server.config.GatewayServerConfiguration;
import com.lyft.data.server.filter.RequestFilter;
import com.lyft.data.server.handler.ServerHandler;
import com.lyft.data.server.servlets.ClientServletImpl;
import com.lyft.data.server.servlets.ProxyServletImpl;

import java.io.Closeable;
import java.io.File;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;
import javax.servlet.Filter;

import lombok.extern.slf4j.Slf4j;

import org.apache.http.util.TextUtils;
import org.eclipse.jetty.http.HttpScheme;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.proxy.ConnectHandler;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class GatewayServer implements Closeable {
  private final Server server;
  private final ServerHandler serverHandler;
  private ServletContextHandler context;

  public GatewayServer(GatewayServerConfiguration config, ServerHandler proxyHandler) {
    this.server = new Server();
    this.server.setStopAtShutdown(true);
    this.serverHandler = proxyHandler;
    this.setupContext(config);
  }

  private void setupContext(GatewayServerConfiguration config) {
    ServerConnector connector = null;
    HttpConfiguration httpConfig = new HttpConfiguration();
    // Increase Header buffer size
    // For prepared statements, Presto sends the prepared query in the header
    // So, the default buffer size of 8kb is insufficient for large queries
    httpConfig.setRequestHeaderSize(1048576); //1MB

    if (config.isSsl()) {
      String keystorePath = config.getKeystorePath();
      String keystorePass = config.getKeystorePass();
      File keystoreFile = new File(keystorePath);

      SslContextFactory sslContextFactory = new SslContextFactory();
      sslContextFactory.setTrustAll(true);
      sslContextFactory.setStopTimeout(TimeUnit.SECONDS.toMillis(15));
      sslContextFactory.setSslSessionTimeout((int) TimeUnit.SECONDS.toMillis(15));

      if (!TextUtils.isBlank(keystorePath)) {
        sslContextFactory.setKeyStorePath(keystoreFile.getAbsolutePath());
        sslContextFactory.setKeyStorePassword(keystorePass);
        sslContextFactory.setKeyManagerPassword(keystorePass);
      }

      httpConfig.setSecureScheme(HttpScheme.HTTPS.asString());
      httpConfig.setSecurePort(config.getLocalPort());
      httpConfig.setOutputBufferSize(32768);

      SecureRequestCustomizer src = new SecureRequestCustomizer();
      src.setStsMaxAge(TimeUnit.SECONDS.toSeconds(2000));
      src.setStsIncludeSubDomains(true);
      httpConfig.addCustomizer(src);
      connector =
          new ServerConnector(
              server,
              new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()),
              new HttpConnectionFactory(httpConfig));
    } else {
      connector = new ServerConnector(server, new HttpConnectionFactory(httpConfig));
    }
    
    // Set up connector properties
    connector.setHost("0.0.0.0");
    connector.setPort(config.getLocalPort());
    connector.setName(config.getName());
    connector.setAccepting(true);
    this.server.addConnector(connector);

    // Set up connection handler
    ConnectHandler connectHandler = new ConnectHandler();
    this.server.setHandler(connectHandler);

    this.context =
        new ServletContextHandler(connectHandler, "/", ServletContextHandler.SESSIONS);

    // Set up proxy servlet
    ProxyServletImpl proxy = new ProxyServletImpl();
    if (serverHandler != null) {
      proxy.setProxyHandler(serverHandler);
    }
    
    ServletHolder proxyServlet = new ServletHolder(config.getName(), proxy);

    proxyServlet.setInitParameter("proxyTo", config.getProxyTo());
    proxyServlet.setInitParameter("prefix", config.getPrefix());
    proxyServlet.setInitParameter("trustAll", config.getTrustAll());
    proxyServlet.setInitParameter("preserveHost", config.getPreserveHost());

    this.context.addServlet(proxyServlet, "/*");

    // Set up client servlet 
    ClientServletImpl client = new ClientServletImpl();
    if (serverHandler != null) {
      client.setServerHandler(serverHandler);
    }

    ServletHolder clientServlet = new ServletHolder("Client Servlet", client);
    
    this.context.addServlet(clientServlet, "/clientServer/*");

    // Adds request filter
    this.context.addFilter(RequestFilter.class, "/*", EnumSet.allOf(DispatcherType.class));
  }

  public void addFilter(Class<? extends Filter> filterClass, String pathSpec) {
    this.context.addFilter(filterClass, pathSpec, EnumSet.allOf(DispatcherType.class));
  }

  public void start() {

    try {
      this.server.start();
    } catch (Exception e) {
      log.error("Error starting proxy server", e);
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void close() {
    try {
      this.server.stop();
    } catch (Exception e) {
      log.error("Could not close the proxy server", e);
    }
  }
}
