package com.lyft.data.server.filter;

import com.lyft.data.baseapp.BaseHandler;
import com.lyft.data.server.handler.ServerHandler;
import com.lyft.data.server.wrapper.MultiReadHttpServletRequest;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import javax.ws.rs.HttpMethod;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequestFilter implements Filter {
  private FilterConfig filterConfig = null;

  public void init(FilterConfig filterConfig) throws ServletException {
    this.filterConfig = filterConfig;
  }

  public void destroy() {
    this.filterConfig = null;
  }

  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    HttpServletRequest req = (HttpServletRequest) request;
    String reqUri = req.getRequestURI();

    if (req.getHeader(BaseHandler.CLIENT_SERVER_REDIRECT) == null 
          && !reqUri.startsWith(BaseHandler.CLIENT_SERVER_PREFIX)
          && ServerHandler.isStatementPath(req.getRequestURI())
          && req.getMethod().equals(HttpMethod.GET)) {
      // Forward to client server initially
      reqUri = ServerHandler.CLIENT_SERVER_PREFIX + reqUri;
      req.getRequestDispatcher(reqUri).forward(request, response);
    } else {
      // We need to convert the ServletRequest to MultiReadRequest, so that we can intercept later
      MultiReadHttpServletRequest multiReadRequest =
          new MultiReadHttpServletRequest((HttpServletRequest) request);
      HttpServletResponseWrapper responseWrapper =
          new HttpServletResponseWrapper((HttpServletResponse) response);
      chain.doFilter(multiReadRequest, responseWrapper);
    }
  }
}
