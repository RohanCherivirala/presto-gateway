package com.lyft.data.baseapp;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.http.HttpServletResponse;

import org.apache.http.HttpHeaders;

public abstract class BaseHandler {
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

  public static final Pattern EXTRACT_BETWEEN_SINGLE_QUOTES = Pattern.compile("'([^\\s']+)'");

  /**
   * Checks if a response uses gzip encoding.
   * @param response Response sent from forwarded server
   * @return If the responses uses gzip encoding
   */
  public static boolean isGZipEncoding(HttpServletResponse response) {
    String contentEncoding = response.getHeader(HttpHeaders.CONTENT_ENCODING);
    return isGZipEncoding(contentEncoding);
  }

  /**
   * Checks if a response uses gzip encoding based on its content enc header.
   * @param contentEncoding Conetnet encoding header
   * @return
   */
  public static boolean isGZipEncoding(String contentEncoding) {
    return contentEncoding != null && contentEncoding.toLowerCase().contains("gzip");
  }

  /**
   * Processes a byte array containing a gzipped string and returns
   * the text contained within it.
   * @param compressed Byte buffer containing gzipped string
   * @return Plain string contained within gzipped buffer
   */
  public static String plainTextFromGz(byte[] compressed) throws IOException {
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
  public static byte[] compressToGz(String str) throws IOException {
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
  public static boolean isCompressed(final byte[] compressed) {
    return (compressed[0] == (byte) (GZIPInputStream.GZIP_MAGIC))
        && (compressed[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8));
  }

  /**
   * Takes in an edited uri and removes the client portion from the tag.
   * @param requestUri Request Uri from client
   * @return Request Uri without the client info
   */
  public static String removeClientFromUri(String requestUri) {
    return requestUri.replace(CLIENT_SERVER_PREFIX, "");
  }
}
