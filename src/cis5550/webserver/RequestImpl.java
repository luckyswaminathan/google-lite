package cis5550.webserver;

import java.util.*;
import java.net.*;
import java.nio.charset.*;

// Provided as part of the framework code

class RequestImpl implements Request {
  String method;
  String url;
  String protocol;
  InetSocketAddress remoteAddr;
  Map<String,String> headers;
  Map<String,String> queryParams;
  Map<String,String> params;
  byte bodyRaw[];
  Server server;
  Response response;
  boolean isSecure;

  RequestImpl(String methodArg, String urlArg, String protocolArg, Map<String,String> headersArg, Map<String,String> queryParamsArg, Map<String,String> paramsArg, InetSocketAddress remoteAddrArg, byte bodyRawArg[], Server serverArg, Response responseArg, boolean isSecureArg) {
    method = methodArg;
    url = urlArg;
    remoteAddr = remoteAddrArg;
    protocol = protocolArg;
    headers = headersArg;
    queryParams = queryParamsArg;
    params = paramsArg;
    bodyRaw = bodyRawArg;
    server = serverArg;
    response = responseArg;
    isSecure = isSecureArg;
  }

  public String requestMethod() {
  	return method;
  }
  public void setParams(Map<String,String> paramsArg) {
    params = paramsArg;
  }
  public int port() {
  	return remoteAddr.getPort();
  }
  public String url() {
  	return url;
  }
  public String protocol() {
  	return protocol;
  }
  public String contentType() {
  	return headers.get("content-type");
  }
  public String ip() {
  	return remoteAddr.getAddress().getHostAddress();
  }
  public String body() {
    return new String(bodyRaw, StandardCharsets.UTF_8);
  }
  public byte[] bodyAsBytes() {
  	return bodyRaw;
  }
  public int contentLength() {
  	return bodyRaw.length;
  }
  public String headers(String name) {
  	return headers.get(name.toLowerCase());
  }
  public Set<String> headers() {
  	return headers.keySet();
  }
  public String queryParams(String param) {
  	return queryParams.get(param);
  }
  public Set<String> queryParams() {
  	return queryParams.keySet();
  }
  public String params(String param) {
    return params.get(param);
  }
  public Map<String,String> params() { return params; }

  public Session session() {
    String sessionId = parseSessionIdFromCookie(headers.get("cookie"));
    SessionImpl session = null;

    // Get session obj if sessionId has been sent
    if (sessionId != null) {
      session = (SessionImpl) Server.server.getSession(sessionId);

      // Check and invalidate expired session
      if (session != null && !session.isValid()) {
        session.invalidate(); // invalidates the session, waiting for Server.server.sessionCleanerExecutor to remove it
        session = null;
      }
    }

    // Check if new session needed to be created, add "Set-Cookie" header to response if so
    if (session == null) {
      session = Server.createSession();
      sessionId = session.id();
      String cookieAttributes = "HttpOnly; SameSite=Strict";
      if (isSecure) {
        cookieAttributes += "; Secure";
      }
      response.header("Set-Cookie", "SessionID=" + sessionId + "; " + cookieAttributes);

      response.header("Set-Cookie", "SessionID=" + sessionId + ";"); // add set-cookie header if new session created
    }

    session.updateAccessTime(); // Indicate that session is now recently accessed
    return session;
  }

  private static String parseSessionIdFromCookie(String cookieHeader) {
    if (cookieHeader != null) {
      String[] cookies = cookieHeader.split(";");
      for (String cookie : cookies) {
        String[] parts = cookie.split("=", 2);
        if (parts[0].trim().equals("SessionID") && parts.length > 1) {
          return parts[1].trim();
        }
      }
    }
    return null;
  }
}
