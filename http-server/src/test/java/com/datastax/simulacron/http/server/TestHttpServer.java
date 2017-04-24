package com.datastax.simulacron.http.server;

import com.datastax.simulacron.server.Server;

public class TestHttpServer {
  public static synchronized void main(String[] args) {
    HttpContainer httpServer = new HttpContainer(8187, true);

    Server nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);

    QueryManager qManager = new QueryManager(nativeServer);
    provisioner.registerWithRouter(httpServer.getRouter());
    qManager.registerWithRouter(httpServer.getRouter());

    SwaggerUI swaggerUI = new SwaggerUI();
    swaggerUI.registerWithRouter(httpServer.getRouter());

    httpServer.start();
    try {
      TestHttpServer.class.wait();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
