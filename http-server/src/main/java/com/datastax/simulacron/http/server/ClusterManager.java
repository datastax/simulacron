package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ClusterMapper;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClusterManager implements HttpListener {
  Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  Server server;

  public ClusterManager(Server server) {
    this.server = server;
  }

  public void provisionCluster(RoutingContext context) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String dcRawString = context.request().getParam("dataCenters");
                StringBuffer response = new StringBuffer();
                ObjectMapper om = ClusterMapper.getMapper();
                if (dcRawString != null) {
                  String[] dcStrs = dcRawString.split(",");
                  int[] dcs = new int[dcStrs.length];
                  for (int i = 0; i < dcStrs.length; i++) {
                    dcs[i] = Integer.parseInt(dcStrs[i]);
                  }
                  Cluster cluster = Cluster.builder().withNodes(dcs).build();
                  CompletableFuture<Cluster> future = server.register(cluster);
                  Cluster registeredCluster = future.get();
                  String clusterStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(registeredCluster);
                  response.append(clusterStr);
                } else {
                  System.out.println("Full body received, length = " + totalBuffer.length());
                  String jsonBody = totalBuffer.toString();
                  try {
                    Cluster cluster = om.readValue(jsonBody, Cluster.class);
                    //insure cluster doesn't exist.
                    //invoke bind for clusters and nodes  start logic.

                  } catch (IOException e) {
                    logger.error("Error decoding json cluster object encountered", e);
                  }
                }
                context.request().response().end(response.toString());
              } catch (Exception e) {
                context
                    .request()
                    .response()
                    .setStatusCode(400)
                    .end("Error encountered proccessing cluster provisioning requests ");
                logger.error("Error encountered proccessing cluster provisioning requests ", e);
              }
            });
  }

  public void getCluster(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Map<Long, Cluster> clusters = this.server.getClusterRegistry();
                ObjectMapper om = ClusterMapper.getMapper();
                StringBuffer response = new StringBuffer();
                String idToFetch = context.request().getParam("clusterId");
                if (idToFetch != null) {
                  Cluster cluster = clusters.get(Long.parseLong(idToFetch));
                  String clusterStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(cluster);
                  response.append(clusterStr);

                } else {
                  //for (Cluster cluster : clusters.values()) {
                  String clusterStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(clusters.values());
                  response.append(clusterStr);
                  //}
                }
                context.request().response().end(response.toString());
              } catch (Exception e) {
                logger.error("Unable to fetch cluster information ", e);
                context
                    .request()
                    .response()
                    .setStatusCode(400)
                    .end("Error encountered fetching cluster information");
              }
            });
  }

  public void registerWithRouter(Router router) {
    router.route(HttpMethod.POST, "/cluster").handler(this::provisionCluster);
    router.route(HttpMethod.GET, "/cluster/:clusterId").handler(this::getCluster);
    router.route(HttpMethod.GET, "/cluster/").handler(this::getCluster);
  }
}
