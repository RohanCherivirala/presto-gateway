package com.lyft.data.server.handler;

import com.google.common.base.Strings;
import com.google.common.io.CharStreams;
import com.lyft.data.baseapp.BaseHandler;

import java.io.IOException;
import java.util.Collection;
import java.util.Enumeration;
import java.util.regex.Matcher;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.util.Callback;

/* Order of control => rewriteTarget, preConnectionHook, postConnectionHook. */
@Slf4j
public class ServerHandler extends BaseHandler {
  protected final int serverApplicationPort;

  public ServerHandler(int serverApplicationPort) {
    this.serverApplicationPort = serverApplicationPort;
  }

  public String rewriteTarget(HttpServletRequest request) {
    // Dont override this unless absolutely needed.
    String backendAddress = "http://localhost:" + serverApplicationPort;

    String targetLocation =
        backendAddress
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");
    return targetLocation;
  }

  /**
   * Request interceptor.
   *
   * @param request {@link HttpServletRequest} sent by client
   * @param proxyRequest ProxyRequest made by proxyServlet
   */
  public void preConnectionHook(HttpServletRequest request, Request proxyRequest) {
    // you may override it.
  }

  /**
   * Response interceptor default.
   *
   * @param request {@link HttpServletRequest} sent by client
   * @param response {@link HttpServletResponse} to sent to target
   * @param buffer Body of response recieved from forwarded server
   * @param offset Offset of response
   * @param length Lenght of response
   * @param callback Callback function
   */
  public void postConnectionHook(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      response.getOutputStream().write(buffer, offset, length);
      callback.succeeded();
    } catch (Throwable var9) {
      callback.failed(var9);
    }
  }

  public void onCompleteHook(
      HttpServletRequest request, 
      HttpServletResponse proxyResponse, 
      Response serverResponse) {
    log.debug("Request reached completion");
  }

  /**
   * Fills in the response body from cache. Used when it is necessary to
   * send a response to a client when it issues a get request.
   * @param req Request sent by client
   * @param resp Response sent back to client
   * @return If the response is filled properly
   */
  public boolean fillResponseForQueryFromCache(HttpServletRequest req, HttpServletResponse resp) {
    return false;
  }

  /**
   * Returns if authorization is enabled.
   */
  public boolean isAuthEnabled() {
    return false;
  }

  /**
   * Handles authorization based on request sent from client.
   * @param request Request sent from client
   * @return If the request is authorized
   */
  public boolean handleAuthRequest(HttpServletRequest request) {
    return true;
  }

  /**
   * Sets a header value within the proxy request.
   * @param proxyRequest Proxy request to be made
   * @param key Header to set
   * @param value Value of header
   */
  protected void setProxyHeader(Request proxyRequest, String key, String value) {
    if (key == null || value == null) {
      return;
    }
    log.debug("Setting header [{}] with value [{}]", key, value);
    proxyRequest.getHeaders().remove(key);
    proxyRequest.header(key, value);
  }

  /**
   * Fills response body based on byte array.
   * @param bytes Bytes to fill response body
   * @param response Response to be sent to client
   * @throws IOException
   */
  protected void fillRepsonseBody(final byte[] bytes, HttpServletResponse response) 
      throws IOException {
    response.setHeader(CONTENT_LENGTH_HEADER, bytes.length + "");
    response.setHeader(CONTENT_TYPE_HEADER, "application/json");
    response.setDateHeader(DATE_HEADER, System.currentTimeMillis());

    response.getOutputStream().write(bytes);
  }

  /**
   * Extract query id from request if possible.
   * @param request {@link HttpServletRequest} sent by client
   * @return Query id
   */
  public String extractQueryIdIfPresent(HttpServletRequest request) {
    return extractQueryIdIfPresent(request.getRequestURI(), request);
  }

  /**
   * Extracts query id from request if possible and specify path to use when doing
   * so (Useful when making calls from {@link ClientServletImpl}).
   * @param request {@link HttpServletRequest} sent by client
   * @return Query id
   */
  public String extractQueryIdIfPresent(String path, HttpServletRequest request) {
    String queryParams = request.getQueryString();
    try {
      String queryText = CharStreams.toString(request.getReader());
      if (!Strings.isNullOrEmpty(queryText)
          && queryText.toLowerCase().contains("system.runtime.kill_query")) {
        // extract and return the queryId
        String[] parts = queryText.split(",");
        for (String part : parts) {
          if (part.contains("query_id")) {
            Matcher m = EXTRACT_BETWEEN_SINGLE_QUOTES.matcher(part);
            if (m.find()) {
              String queryQuoted = m.group();
              if (!Strings.isNullOrEmpty(queryQuoted) && queryQuoted.length() > 0) {
                return queryQuoted.substring(1, queryQuoted.length() - 1);
              }
            }
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
    if (path == null) {
      return null;
    }
    String queryId = null;

    log.debug("trying to extract query id from path [{}] or queryString [{}]", path, queryParams);
    if (path.startsWith(V1_STATEMENT_PATH) || path.startsWith(V1_QUERY_PATH)) {
      String[] tokens = path.split("/");
      if (tokens.length >= 4) {
        if (path.contains("queued")
            || path.contains("scheduled")
            || path.contains("executing")
            || path.contains("partialCancel")) {
          queryId = tokens[4];
        } else {
          queryId = tokens[3];
        }
      }
    } else if (path.startsWith(PRESTO_UI_PATH)) {
      queryId = queryParams;
    }
    log.debug("query id in url [{}]", queryId);
    return queryId;
  }

  /**
   * Checks if a path is white-listed as a path to be sent to
   * the presto-cluster.
   * @param path Path of request
   * @return If a path is white-listed
   */
  public boolean isPathWhiteListed(String path) {
    return path.startsWith(V1_STATEMENT_PATH)
        || path.startsWith(V1_QUERY_PATH)
        || path.startsWith(PRESTO_UI_PATH)
        || path.startsWith(V1_INFO_PATH)
        || path.startsWith(UI_API_STATS_PATH);
  }

  /**
   * Takes in an edited uri and removes the client portion from the tag.
   * @param requestUri Request Uri from client
   * @return Request Uri without the client info
   */
  public static String removeClientFromUri(String requestUri) {
    return requestUri.substring(CLIENT_SERVER_PREFIX.length());
  }

  /**
   * Checks if path is a statement path.
   * @param path Path of request
   * @return If the path is a statement path
   */
  public static boolean isStatementPath(String path) {
    return path.startsWith(V1_STATEMENT_PATH);
  }

  // Methods for debugging responses
  public void debugLogHeaders(Response response) {
    if (log.isDebugEnabled()) {
      log.debug("-------Server Response HTTP headers---------");
      response.getHeaders().stream().forEach(head -> {
        log.debug(head.toString());
      });
    }
  }

  public void debugLogHeaders(HttpServletRequest request) {
    if (log.isDebugEnabled()) {
      log.debug("Request URI: [{}]", request.getRequestURI());
      log.debug("-------HttpServletRequest headers---------");
      Enumeration<String> headers = request.getHeaderNames();
      while (headers.hasMoreElements()) {
        String header = headers.nextElement();
        log.debug(header + "->" + request.getHeader(header));
      }
    }
  }

  public void debugLogHeaders(HttpServletResponse response) {
    if (log.isDebugEnabled()) {
      log.debug("-------HttpServletResponse headers---------");
      Collection<String> headers = response.getHeaderNames();
      for (String header : headers) {
        log.debug(header + "->" + response.getHeader(header));
      }
    }
  }

  public void debugLogHeaders(Request proxyRequest) {
    if (log.isDebugEnabled()) {
      log.debug("-------Request proxyRequest headers---------");
      HttpFields httpFields = proxyRequest.getHeaders();
      log.debug(httpFields.toString());
    }
  }
}
