package com.datastax.simulacron.http.server;

import java.util.concurrent.TimeUnit;

public class HttpContainerTest {

    public static void main(String[] args){
        HttpContainer server = new HttpContainer(8187, true);
        ClusterManager provisioner = new ClusterManager();
        QueryManager qManager = new QueryManager();
        provisioner.registerWithRouter(server.getRouter());
        qManager.registerWithRouter(server.getRouter());
        server.start();
        try {
            TimeUnit.MINUTES.sleep(5);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
