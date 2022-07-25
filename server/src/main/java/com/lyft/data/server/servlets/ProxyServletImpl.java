package com.lyft.data.server.servlets;

import com.lyft.data.server.handler.ServerHandler;

import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.proxy.ProxyServlet;
import org.eclipse.jetty.util.Callback;
import org.eclipse.jetty.util.ssl.SslContextFactory;

@Slf4j
public class ProxyServletImpl extends ProxyServlet.Transparent {
  private ServerHandler proxyHandler;

  public void setProxyHandler(ServerHandler proxyHandler) {
    this.proxyHandler = proxyHandler;
    // This needs to be high as external clients may take longer to connect.
    this.setTimeout(TimeUnit.MINUTES.toMillis(1));
  }

  // Overriding this method to support ssl
  @Override
  protected HttpClient newHttpClient() {
    SslContextFactory sslFactory = new SslContextFactory();
    sslFactory.setTrustAll(true);
    sslFactory.setStopTimeout(TimeUnit.SECONDS.toMillis(15));
    sslFactory.setSslSessionTimeout((int) TimeUnit.SECONDS.toMillis(15));

    HttpClient httpClient = new HttpClient(sslFactory);
    httpClient.setMaxConnectionsPerDestination(10000);
    httpClient.setConnectTimeout(TimeUnit.SECONDS.toMillis(60));
    // Increase Header buffer size
    // For prepared statements, Presto sends the prepared query in the header
    // So, the default buffer size of 4kb is insufficient for large queries
    httpClient.setRequestBufferSize(1048576); //1MB
    return httpClient;
  }

  /** Customize the headers of forwarding proxy requests. */
  @Override
  protected void addProxyHeaders(HttpServletRequest request, Request proxyRequest) {
    super.addProxyHeaders(request, proxyRequest);
    if (proxyHandler != null) {
      proxyHandler.preConnectionHook(request, proxyRequest);
    }
  }

  @Override
  protected String rewriteTarget(HttpServletRequest request) {
    String target = null;
    if (proxyHandler != null) {
      target = proxyHandler.rewriteTarget(request);
    }
    if (target == null) {
      target = super.rewriteTarget(request);
    }
    log.debug("Target : " + target);
    return target;
  }

  /**
   * Customize the response returned from remote server.
   *
   * @param request
   * @param response
   * @param proxyResponse
   * @param buffer
   * @param offset
   * @param length
   * @param callback
   */
  @Override
  protected void onResponseContent(
      HttpServletRequest request,
      HttpServletResponse response,
      Response proxyResponse,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      if (this._log.isDebugEnabled()) {
        this._log.debug(
            "[{}] proxying content to downstream: [{}] bytes", this.getRequestId(request), length);
      }

      this._log.debug("\n\nPROXY HANDLER IS {}\n\n", proxyHandler == null ? "NULL" : "NOT NULL");
      this._log.debug("\nOFFSET: {} \n", offset);

      if (this.proxyHandler != null) {
        proxyHandler.postConnectionHook(request, response, buffer, offset, length, callback);
      } else {
        super.onResponseContent(request, response, proxyResponse, buffer, offset, length, callback);
      }
    } catch (Throwable var9) {
      callback.failed(var9);
    }
  }

  @Override
  protected void onProxyResponseSuccess(
      HttpServletRequest clientRequest, 
      HttpServletResponse proxyResponse, 
      Response serverResponse) {
    if (proxyHandler != null) {
      proxyHandler.onCompleteHook(clientRequest, proxyResponse, serverResponse);
    }

    super.onProxyResponseSuccess(clientRequest, proxyResponse, serverResponse);
  }
}
