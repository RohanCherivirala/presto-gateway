package com.lyft.data.server.handler;

import com.google.common.base.Strings;
import com.google.common.io.CharStreams;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.client.api.Response;
import org.eclipse.jetty.http.HttpFields;
import org.eclipse.jetty.util.Callback;

/* Order of control => rewriteTarget, preConnectionHook, postConnectionHook. */
@Slf4j
public class ServerHandler {
  public static final String PROXY_TARGET_HEADER = "proxytarget";
  public static final String V1_STATEMENT_PATH = "/v1/statement";
  public static final String V1_QUERY_PATH = "/v1/query";
  public static final String V1_INFO_PATH = "/v1/info";
  public static final String UI_API_STATS_PATH = "/ui/api/stats";
  public static final String PRESTO_UI_PATH = "/ui";
  public static final String CLIENT_SERVER_PREFIX = "/clientServer";

  // Add support for Trino
  public static final String USER_HEADER = "X-Trino-User";
  public static final String ALTERNATE_USER_HEADER = "X-Presto-User";
  public static final String SOURCE_HEADER = "X-Trino-Source";
  public static final String ALTERNATE_SOURCE_HEADER = "X-Presto-Source";
  public static final String ROUTING_GROUP_HEADER = "X-Trino-Routing-Group";
  public static final String ALTERNATE_ROUTING_GROUP_HEADER = "X-Presto-Routing-Group";
  public static final String CLIENT_TAGS_HEADER = "X-Trino-Client-Tags";
  public static final String ALTERNATE_CLIENT_TAGS_HEADER = "X-Presto-Client-Tags";
  public static final String CLIENT_SERVER_REDIRECT = "Client-Server-Redirected";

  public static final String ADHOC_ROUTING_GROUP = "adhoc";
  public static final int QUERY_TEXT_LENGTH_FOR_HISTORY = 200;

  public static final String CONTENT_LENGTH_HEADER = "Content-Length";
  public static final String CONTENT_TYPE_HEADER = "Content-Type";
  public static final String DATE_HEADER = "Date";

  private static final Pattern EXTRACT_BETWEEN_SINGLE_QUOTES = Pattern.compile("'([^\\s']+)'");

  /**
   * Rewrites target URL.
   * @param request {@link HttpServletRequest} sent by client
   * @return String containing new target.
   */
  public String rewriteTarget(HttpServletRequest request) {
    return null;
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
   * Checks if a response uses gzip encoding.
   * @param response Response sent from forwarded server
   * @return If the responses uses gzip encoding
   */
  protected boolean isGZipEncoding(HttpServletResponse response) {
    String contentEncoding = response.getHeader(HttpHeaders.CONTENT_ENCODING);
    return contentEncoding != null && contentEncoding.toLowerCase().contains("gzip");
  }

  /**
   * Processes a byte array containing a gzipped string and returns
   * the text contained within it.
   * @param compressed Byte buffer containing gzipped string
   * @return Plain string contained within gzipped buffer
   */
  protected String plainTextFromGz(byte[] compressed) throws IOException {
    final StringBuilder outStr = new StringBuilder();
    if ((compressed == null) || (compressed.length == 0)) {
      return "";
    }

    if (isCompressed(compressed)) {
      final GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(compressed));
      final BufferedReader bufferedReader =
          new BufferedReader(new InputStreamReader(gis, Charset.defaultCharset()));
          
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        outStr.append(line);
      }
      gis.close();
    } else {
      outStr.append(compressed);
    }
    
    return outStr.toString();
  }

  /**
   * Compresses string into Gzip format.
   * @param str String to compress
   * @return Byte array containing compressed bytes
  */
  protected byte[] compressToGz(String str) throws IOException {
    if (str == null || str.length() == 0) {
      return new byte[] {};
    }

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(byteStream);
    gzip.write(str.getBytes(Charset.defaultCharset()));
    gzip.close();

    return byteStream.toByteArray();
  }

  /**
   * Checks if a byte array is compressed.
   * @param compressed Byte array to check
   * @return If byte array is compressed
   */
  protected boolean isCompressed(final byte[] compressed) {
    return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
        && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
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
