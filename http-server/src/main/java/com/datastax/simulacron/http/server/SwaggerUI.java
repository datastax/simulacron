package com.datastax.simulacron.http.server;

import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;

public class SwaggerUI implements HttpListener {

  @Override
  public void registerWithRouter(Router router) {
    StaticHandler staticHandler = StaticHandler.create();
    staticHandler.setWebRoot("META-INF/resources/webjars");
    router.route("/static/*").handler(staticHandler);

    StaticHandler swaggerHandler = StaticHandler.create().setWebRoot("webroot/swagger");
    router.route("/doc/*").handler(swaggerHandler);
  }
}
