package com.datastax.simulacron.http.server;

import com.datastax.simulacron.server.Server;

import java.util.concurrent.TimeUnit;

public class TestHttpServer {
  public static void main(String[] args) {
    HttpContainer httpServer = new HttpContainer(8187, true);

    Server nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);

    QueryManager qManager = new QueryManager();
    provisioner.registerWithRouter(httpServer.getRouter());
    qManager.registerWithRouter(httpServer.getRouter());
    httpServer.start();
    try {
      TimeUnit.MINUTES.sleep(5);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
