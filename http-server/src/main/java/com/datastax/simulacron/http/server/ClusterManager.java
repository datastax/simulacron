package com.datastax.simulacron.http.server;

import com.datastax.simulacron.cluster.Cluster;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManager implements HttpListener{
    Logger logger = LoggerFactory.getLogger(ClusterManager.class);
    public static ConcurrentHashMap<String, Cluster> clusters = new ConcurrentHashMap<String, Cluster>();

    public void provisionCluster(RoutingContext context){

        context.request().bodyHandler(totalBuffer -> {
            System.out.println("Full body received, length = " + totalBuffer.length());
            String jsonBody = totalBuffer.toString();
            ObjectMapper om = new ObjectMapper();
            om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
            try {
                Cluster cluster = om.readValue(jsonBody, Cluster.class);
                clusters.put(cluster.name(), cluster);
                //insure cluster doesn't exist.
                //invoke bind for clusters and nodes  start logic.

            }
            catch (IOException e){
                logger.error("Error decoding json cluster object encountered", e);
            }

        });
        context.request().response().end("Invoking cluster creation");
    }

    public void registerWithRouter(Router router){
        router.route(HttpMethod.POST,"/cluster").handler(this::provisionCluster);
    }



}
