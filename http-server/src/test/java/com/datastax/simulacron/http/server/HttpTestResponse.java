package com.datastax.simulacron.http.server;

import io.vertx.core.http.HttpClientResponse;

public class HttpTestResponse {
  public HttpClientResponse response;
  public String body;

  public HttpTestResponse(HttpClientResponse response, String body) {
    this.response = response;
    this.body = body;
  }
}
