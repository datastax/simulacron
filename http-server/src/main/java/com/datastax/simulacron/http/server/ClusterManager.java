package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ClusterMapper;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ClusterManager implements HttpListener {
  Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  Server server;

  public ClusterManager(Server server) {
    this.server = server;
  }

  /**
   * This is an async callback that will be invoked whenever a request to /cluster is posted. It
   * will extract parameters from the request, and invoke the native server provisioning logic. On
   * completion it will return the created cluster on success, or an error message of failure.
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>POST http://iphere:porthere/cluster?dataCenters=3,4,9 this will create a cluster with 3
   * datacenters, with 3, 4 and 9 nodes.
   *
   * <p>POST POST http://iphere:porthere/cluster Containing a json body {@code { "name" : "1", "id"
   * : 1, "data_centers" : [ { "name" : "dc1", "id" : 0, "nodes" : [ { "name" : "node1", "id" : 0,
   * "address" : "127.0.1.2:9042" } ] } ] } } This will create a cluster defined exactly as
   * perscrbied in the json
   *
   * @param context RoutingContext provided by vertx
   */
  public void provisionCluster(RoutingContext context) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String dcRawString = context.request().getParam("dataCenters");
                StringBuffer response = new StringBuffer();
                ObjectMapper om = ClusterMapper.getMapper();
                Cluster cluster = null;
                //General parameters were provided for us.
                if (dcRawString != null) {
                  String[] dcStrs = dcRawString.split(",");
                  int[] dcs = new int[dcStrs.length];
                  for (int i = 0; i < dcStrs.length; i++) {
                    dcs[i] = Integer.parseInt(dcStrs[i]);
                    cluster = Cluster.builder().withNodes(dcs).build();
                  }
                } else {
                  //A specific cluster object was provided.
                  // We could add special handling here, but I'm not sure it's worth it
                  String jsonBody = totalBuffer.toString();
                  cluster = om.readValue(jsonBody, Cluster.class);
                }
                CompletableFuture<Cluster> future = server.register(cluster);
                future.handle(
                    (completedCluster, ex) -> {
                      if (ex == null) {
                        try {
                          String clusterStr =
                              om.writerWithDefaultPrettyPrinter()
                                  .writeValueAsString(completedCluster);
                          response.append(clusterStr);
                        } catch (JsonProcessingException jpex) {
                          logger.error(
                              "Error encountered when attempting to form json response", jpex);
                        }
                      }
                      if (ex != null) {
                        handleClusterError(ex, "provision", context);

                        return null;
                      }
                      context
                          .request()
                          .response()
                          .putHeader("content-type", "application/json")
                          .setStatusCode(201)
                          .end(response.toString());
                      return completedCluster;
                    });
              } catch (Exception e) {
                handleClusterError(e, "provision", context);
              }
            });
  }

  /**
   * This is an async callback that will be invoked whenever a request to /cluster is submited with
   * GET. When a clusterId is provided in the format of /cluster/:clusterId, we will fetch that
   * specific id.
   *
   * <p>Example supported HTTP requests
   *
   * <p>GET http://iphere:porthere/cluster/ Will return all provisioned clusters
   *
   * <p>GET http://iphere:porthere/cluster/:id Will return the cluster with the provided id
   *
   * @param context RoutingContext Provided by vertx
   */
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

                  String clusterStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(clusters.values());
                  response.append(clusterStr);
                }
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(200)
                    .end(response.toString());
              } catch (Exception e) {
                handleClusterError(e, "retrieve", context);
              }
            });
  }

  private void handleClusterError(Throwable e, String operation, RoutingContext context) {
    logger.error("Unable to " + operation + " cluster", e);
    context
        .request()
        .response()
        .setStatusCode(400)
        .end(
            "Error encountered while attempting to "
                + operation
                + " cluster"
                + "\n"
                + "Please see error logs for more details");
  }

  /**
   * This method handles the registration of the various routes responsible for setting and
   * retrieving cluster information via http.
   *
   * @param router
   */
  public void registerWithRouter(Router router) {
    router.route(HttpMethod.POST, "/cluster").handler(this::provisionCluster);

    router.route(HttpMethod.GET, "/cluster/:clusterId").handler(this::getCluster);
    router.route(HttpMethod.GET, "/cluster/").handler(this::getCluster);
  }
}
