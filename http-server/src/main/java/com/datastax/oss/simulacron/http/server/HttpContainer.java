/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.http.server;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpContainer {
  Logger logger = LoggerFactory.getLogger(HttpContainer.class);

  public static final int DEFAULT_PORT = 8187;

  private String host;
  private int port = DEFAULT_PORT;
  private boolean enableLogging = true;
  private Router router = null;
  private HttpServer server = null;
  private Vertx vertx = null;
  public static ConcurrentHashMap<String, Object> primedQueries =
      new ConcurrentHashMap<String, Object>();

  public HttpContainer(int port, boolean enableLogging) {
    this("localhost", port, enableLogging);
  }

  public HttpContainer(String host, int port, boolean enableLogging) {
    this.host = host;
    this.port = port;
    this.enableLogging = enableLogging;
    vertx = Vertx.vertx();
    HttpServerOptions options = new HttpServerOptions().setLogActivity(this.enableLogging);
    server = vertx.createHttpServer(options);
    router = Router.router(vertx);
  }

  public CompletableFuture<Void> start() {
    server.requestHandler(router::accept);
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.listen(
        port,
        host,
        res -> {
          if (res.failed()) {
            future.completeExceptionally(res.cause());
          } else {
            future.complete(null);
          }
        });

    return future;
  }

  public void stop() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.close(
        res -> {
          future.complete(null);
        });
    try {
      future.get();
    } catch (Exception e) {
      logger.error("Error encountered during shutdown", e);
    }
  }

  public String getHost() {
    return host;
  }

  public int getPort() {
    return port;
  }

  public void addRoute(Handler<RoutingContext> handler, String path, HttpMethod method) {
    router.route(method, path).handler(handler);
  }

  public Router getRouter() {
    return router;
  }
}
