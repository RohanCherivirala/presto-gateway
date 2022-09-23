package com.lyft.data.server.servlets;

import com.lyft.data.server.handler.ServerHandler;
import com.lyft.data.server.wrapper.MultiReadHttpServletRequest;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.HttpMethod;

public class ClientServletImpl extends HttpServlet {
  public static final int GET_RESPONSE_DELAY = 1;

  private ServerHandler serverHandler;

  public void setServerHandler(ServerHandler serverHandler) {
    this.serverHandler = serverHandler;
  }

  @Override
  protected void service(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {   
    if (req.getMethod().equals(HttpMethod.GET) 
        && ServerHandler.isStatementPath(
            ServerHandler.removeClientFromUri(req.getRequestURI()))) {
      super.service(req, resp);
    } else {
      redirectRequest(req, resp);
    }
  }

  /**
   * Processes GET requests sent by clients with the intention of gather information
   * about a presto query.
   * Returns the response that was first recieved by the client until all information
   * has been gathered from the cluster.
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws ServletException, IOException {
    /*
     * TODO: Delay GET responses if completed is false... I tried using a scheduled thread pool
     * and delaying reponses by a second but that didn't work as Jetty already returned some kind of
     * response so we might have to make this thread itself sleep or look into some other option.
     */
    Boolean completed = Boolean.FALSE;
    if (serverHandler.fillResponseForQueryFromCache(req, resp, completed)) {
      resp.setStatus(HttpServletResponse.SC_OK);
    } else {
      resp.sendError(HttpServletResponse.SC_NOT_FOUND);
    }

    resp.flushBuffer();
  }

  /**
   * Redirects the request to be handled by the proxy handler.
   * @param req {@link HttpServletRequest} sent by client
   * @param resp  {@link HttpServletResponse} to send to client
   */
  private void redirectRequest(HttpServletRequest req, HttpServletResponse resp) 
      throws ServletException, IOException {
    String newUri = ServerHandler.removeClientFromUri(req.getRequestURI());

    ((MultiReadHttpServletRequest)req).addHeader(ServerHandler.CLIENT_SERVER_REDIRECT, "true");
    req.getRequestDispatcher(newUri).forward(req, resp);
  }
}
