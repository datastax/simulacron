package com.datastax.simulacron.http.server;

import io.vertx.ext.web.Router;

public interface HttpListener {

  public void registerWithRouter(Router router);
}
