package com.datastax.simulacron.http.server;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public class HttpContainer {
  Logger logger = LoggerFactory.getLogger(HttpContainer.class);

  public static final int DEFAULT_PORT = 8187;

  private int port = DEFAULT_PORT;
  private boolean enableLogging = true;
  private Router router = null;
  private HttpServer server = null;
  private Vertx vertx = null;
  public static ConcurrentHashMap<String, Object> primedQueries =
      new ConcurrentHashMap<String, Object>();

  public HttpContainer(int port, boolean enableLogging) {
    this.port = port;
    this.enableLogging = enableLogging;
    vertx = Vertx.vertx();
    HttpServerOptions options = new HttpServerOptions().setLogActivity(this.enableLogging);
    server = vertx.createHttpServer(options);
    router = Router.router(vertx);
  }

  public void start() {

    server.requestHandler(router::accept);
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.listen(
        port,
        res -> {
          future.complete(null);
        });
    try {
      future.get();
    } catch (Exception e) {
      logger.error("Error encountered during Start", e);
    }
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

  public void addRoute(Handler<RoutingContext> handler, String path, HttpMethod method) {
    router.route(method, path).handler(handler);
  }

  public Router getRouter() {
    return router;
  }
}