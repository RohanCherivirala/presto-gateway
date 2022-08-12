package com.lyft.data.gateway.ha.handler;

import com.codahale.metrics.Meter;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Strings;
import com.google.common.cache.CacheLoader.InvalidCacheLoadException;
import com.google.common.io.CharStreams;
import com.lyft.data.gateway.ha.router.CachingDatabaseManager;
import com.lyft.data.gateway.ha.router.QueryHistoryManager;
import com.lyft.data.gateway.ha.router.RoutingManager;
import com.lyft.data.server.handler.ServerHandler;
import com.lyft.data.server.wrapper.MultiReadHttpServletRequest;

import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.util.Callback;

@Slf4j
public class QueryIdCachingServerHandler extends ServerHandler {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private final RoutingManager routingManager;
  private final QueryHistoryManager queryHistoryManager;
  private final CachingDatabaseManager cachingDatabaseManager;

  private final Meter requestMeter;

  public QueryIdCachingServerHandler(
      QueryHistoryManager queryHistoryManager,
      RoutingManager routingManager,
      CachingDatabaseManager cachingDatabaseManager,
      int serverApplicationPort,
      Meter requestMeter) {
    super(serverApplicationPort);
    this.queryHistoryManager = queryHistoryManager;
    this.routingManager = routingManager;
    this.cachingDatabaseManager = cachingDatabaseManager;
    this.requestMeter = requestMeter;
  }

  @Override
  public String rewriteTarget(HttpServletRequest request) {
    debugLogHeaders(request);
    log.debug("\n\nREWRITING TARGET\n\n");

    /* Here comes the load balancer / gateway */
    String backendAddress = "http://localhost:" + serverApplicationPort;

    // Only load balance presto query APIs.
    if (isPathWhiteListed(request.getRequestURI())) {
      String queryId = extractQueryIdIfPresent(request);

      // Find query id and get url from cache
      if (!Strings.isNullOrEmpty(queryId)) {
        backendAddress = routingManager.findBackendForQueryId(queryId);
      } else {
        String routingGroup = Optional.ofNullable(request.getHeader(ROUTING_GROUP_HEADER))
            .orElse(request.getHeader(ALTERNATE_ROUTING_GROUP_HEADER));
        // Fall back on client tags for routing
        if (Strings.isNullOrEmpty(routingGroup)) {
          routingGroup = Optional.ofNullable(request.getHeader(CLIENT_TAGS_HEADER))
            .orElse(request.getHeader(ALTERNATE_CLIENT_TAGS_HEADER));
        }
        if (!Strings.isNullOrEmpty(routingGroup)) {
          // This falls back on adhoc backend if there are no cluster found for the routing group.
          backendAddress = routingManager.provideBackendForRoutingGroup(routingGroup);
        } else {
          backendAddress = routingManager.provideAdhocBackend();
        }
      }
      // set target backend so that we could save queryId to backend mapping later.
      ((MultiReadHttpServletRequest) request).addHeader(PROXY_TARGET_HEADER, backendAddress);
    }

    if (isAuthEnabled() && request.getHeader("Authorization") != null) {
      if (!handleAuthRequest(request)) {
        // This implies the AuthRequest was not authenticated, hence we error out from here.
        log.info("Could not authenticate Request: " + request.toString());
        return null;
      }
    }

    String targetLocation =
        backendAddress
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    String originalLocation =
        request.getScheme()
            + "://"
            + request.getRemoteHost()
            + ":"
            + request.getServerPort()
            + request.getRequestURI()
            + (request.getQueryString() != null ? "?" + request.getQueryString() : "");

    // 6sense: Changed to debug to save sumo logic expenses
    log.debug("Rerouting [{}]--> [{}]", originalLocation, targetLocation);
    return targetLocation;
  }

  @Override
  public void preConnectionHook(HttpServletRequest request, Request proxyRequest) {
    if (request.getMethod().equals(HttpMethod.POST)
        && request.getRequestURI().startsWith(V1_STATEMENT_PATH)) {
      requestMeter.mark();
      try {
        String requestBody = CharStreams.toString(request.getReader());
        // 6sense: Changed to debug to save sumo logic expenses
        log.debug(
            "Processing request endpoint: [{}], payload: [{}]",
            request.getRequestURI(),
            requestBody);
        debugLogHeaders(request);
      } catch (Exception e) {
        log.warn("Error fetching the request payload", e);
      }
    }
  }

  // Code used to read/edit response
  /*
  // This header is only contained on complete responses
  if (response.containsHeader(CONTENT_LENGTH_HEADER)) {
    try {
      if (isGZipEncoding) {
        output = plainTextFromGz(buffer);
      } else {
        output = new String(buffer);
      }
    } catch (Exception e) {
      log.error("Error extracting query payload from request", e);
    }
          
          length = buffer.length;
          response.setContentLengthLong(length);
          log.debug("New buf len: {}", length);
        }

        log.debug("\n\nNo Error Occurred!!!\n\n");
      } else {
        int errorCode = root.at("/error/errorCode").asInt();
        String errorName = root.at("/error/errorName").asText();
        String errorType = root.at("/error/errorType").asText();
  
        log.debug("\n\nError Details:");
        log.debug(String.format("Error Code: %s ErrorName: %s Error Type: %s\n\n", 
                          errorCode, errorName, errorType));
      }
    } catch (JsonParseException | EOFException e) {
      log.debug("Response does not contain complete information");
    }
  }
   */


  @Override
  public void postConnectionHook(
      HttpServletRequest request,
      HttpServletResponse response,
      byte[] buffer,
      int offset,
      int length,
      Callback callback) {
    try {
      String requestPath = request.getRequestURI();

      if (requestPath.startsWith(V1_STATEMENT_PATH)
          && request.getMethod().equals(HttpMethod.POST)) {
        boolean isGZipEncoding = isGZipEncoding(response);
        String output = "";
        
        if (Strings.isNullOrEmpty(output)) {
          if (isGZipEncoding) {
            output = plainTextFromGz(buffer);
          } else {
            output = new String(buffer);
          }
        }

        log.debug("Response output [{}]", output);

        // Store query information used to start call
        QueryHistoryManager.QueryDetail queryDetail = getQueryDetailsFromRequest(request);
        log.debug("Proxy destination : {}", queryDetail.getBackendUrl());

        if (response.getStatus() == HttpStatus.OK_200) {
          HashMap<String, String> results = OBJECT_MAPPER.readValue(output, HashMap.class);
          queryDetail.setQueryId(results.get("id"));

          if (!Strings.isNullOrEmpty(queryDetail.getQueryId())) {
            routingManager.setBackendForQueryId(
                queryDetail.getQueryId(), queryDetail.getBackendUrl());

            log.debug(
                "QueryId [{}] mapped with proxy [{}]",
                queryDetail.getQueryId(),
                queryDetail.getBackendUrl());

            // Caching response sent
            cachingDatabaseManager.set(queryDetail.getQueryId()
                  + CachingDatabaseManager.STALL_RESPONSE_SUFFIX, output);

            // Saving history at gateway.
            queryHistoryManager.submitQueryDetail(queryDetail);
          } else {
            log.debug("QueryId [{}] could not be cached", queryDetail.getQueryId());
          }
        } else {
          log.error(
              "Non OK HTTP Status code with response [{}] , Status code [{}]",
              output,
              response.getStatus());
        }
      } else {
        log.debug("SKIPPING For {}", requestPath);
      }     
    } catch (Exception e) {
      log.error("Error in proxying falling back to super call", e);
    }

    super.postConnectionHook(request, response, buffer, offset, length, callback);
  }

  @Override
  public boolean fillResponseForQueryFromCache(HttpServletRequest req, HttpServletResponse resp) {
    String queryId = extractQueryIdIfPresent(removeClientFromUri(req.getRequestURI()), req);

    try {
      String responseBody = cachingDatabaseManager.get(queryId 
          + CachingDatabaseManager.STALL_RESPONSE_SUFFIX);

      if (Strings.isNullOrEmpty(responseBody)) {
        throw new InvalidCacheLoadException("No matching entry in cache");
      }

      fillRepsonseBody(responseBody.getBytes(Charset.defaultCharset()), resp);

      return true;
    } catch (Exception e) {
      log.error("Error occured when returning response from cache for [{}]", queryId, e);
    }

    return false;
  }

  protected QueryHistoryManager.QueryDetail getQueryDetailsFromRequest(HttpServletRequest request)
      throws IOException {
    QueryHistoryManager.QueryDetail queryDetail = new QueryHistoryManager.QueryDetail();
    queryDetail.setBackendUrl(request.getHeader(PROXY_TARGET_HEADER));
    queryDetail.setCaptureTime(System.currentTimeMillis());
    queryDetail.setUser(Optional.ofNullable(request.getHeader(USER_HEADER))
            .orElse(request.getHeader(ALTERNATE_USER_HEADER)));
    queryDetail.setSource(Optional.ofNullable(request.getHeader(SOURCE_HEADER))
            .orElse(request.getHeader(ALTERNATE_SOURCE_HEADER)));
    String queryText = CharStreams.toString(request.getReader());
    queryDetail.setQueryText(
        queryText.length() > QUERY_TEXT_LENGTH_FOR_HISTORY
            ? queryText.substring(0, QUERY_TEXT_LENGTH_FOR_HISTORY) + "..."
            : queryText);
    return queryDetail;
  }
}
