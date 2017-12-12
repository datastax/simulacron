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

import static com.datastax.oss.simulacron.http.server.HttpUtils.handleError;
import static com.datastax.oss.simulacron.http.server.HttpUtils.handleMessage;

import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.datastax.oss.simulacron.server.ServerOptions;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClusterManager implements HttpListener {
  private final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  private final Server server;
  private final ObjectMapper om = ObjectMapperHolder.getMapper();

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
   * <p>POST http://iphere:porthere/cluster?data_centers=3,4,9 this will create a cluster with 3
   * datacenters, with 3, 4 and 9 nodes.
   *
   * <p>POST POST http://iphere:porthere/cluster Containing a json body {@code { "name" : "1", "id"
   * : 1, "data_centers" : [ { "name" : "dc1", "id" : 0, "nodes" : [ { "name" : "node1", "id" : 0,
   * "address" : "127.0.1.2:9042" } ] } ] } } This will create a cluster defined exactly as
   * perscrbied in the json
   *
   * @param context RoutingContext provided by vertx
   */
  private void provisionCluster(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String dcRawString = context.request().getParam("data_centers");
                String dseVersion = context.request().getParam("dse_version");
                String cassandraVersion = context.request().getParam("cassandra_version");
                String numTokensParam = context.request().getParam("num_tokens");
                int numTokens = numTokensParam != null ? Integer.parseInt(numTokensParam) : 1;
                String activityLog = context.request().getParam("activity_log");
                Boolean activityLogEnabled =
                    activityLog != null ? Boolean.parseBoolean(activityLog) : null;
                String name = context.request().getParam("name");
                StringBuilder response = new StringBuilder();
                ClusterSpec cluster = null;
                // General parameters were provided for us.
                if (dcRawString != null) {
                  String[] dcStrs = dcRawString.split(",");
                  int[] dcs = new int[dcStrs.length];
                  for (int i = 0; i < dcStrs.length; i++) {
                    dcs[i] = Integer.parseInt(dcStrs[i]);
                    cluster =
                        ClusterSpec.builder()
                            .withNodes(dcs)
                            .withDSEVersion(dseVersion)
                            .withCassandraVersion(cassandraVersion)
                            .withName(name)
                            .withNumberOfTokens(numTokens)
                            .build();
                  }
                } else {
                  // A specific cluster object was provided.
                  // We could add special handling here, but I'm not sure it's worth it
                  String jsonBody = totalBuffer.toString();
                  cluster = om.readValue(jsonBody, ClusterSpec.class);
                }
                CompletionStage<BoundCluster> future =
                    server.registerAsync(
                        cluster,
                        ServerOptions.builder()
                            .withActivityLoggingEnabled(activityLogEnabled)
                            .build());
                future.whenComplete(
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
                        handleError(new ErrorMessage(ex.getMessage(), 400), context);
                      } else {
                        context
                            .request()
                            .response()
                            .putHeader("content-type", "application/json")
                            .setStatusCode(201)
                            .end(response.toString());
                      }
                    });
              } catch (Exception e) {
                handleError(new ErrorMessage(e.getMessage(), 400), context);
              }
            });
  }

  private void unregisterCluster(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            b -> {
              try {
                CompletionStage<Integer> future;
                String idOrNameToFetch = context.request().getParam("clusterIdOrName");
                if (idOrNameToFetch == null) {
                  future = server.unregisterAllAsync();
                } else {
                  Optional<Long> clusterId =
                      HttpUtils.getClusterIdFromIdOrName(server, idOrNameToFetch);
                  if (clusterId.isPresent()) {
                    future = server.unregisterAsync(clusterId.get()).thenApply(__ -> 1);
                  } else {
                    handleError(
                        new ErrorMessage(
                            "No cluster registered with id or name " + idOrNameToFetch + ".", 404),
                        context);
                    return;
                  }
                }

                future.whenComplete(
                    (count, ex) -> {
                      if (ex != null) {
                        handleError(new ErrorMessage(ex, 400), context);
                      } else {
                        if (idOrNameToFetch == null) {
                          handleMessage(
                              new Message("All (" + count + ") clusters unregistered.", 202),
                              context);
                        } else {
                          handleMessage(
                              new Message("Cluster " + idOrNameToFetch + " unregistered.", 202),
                              context);
                        }
                      }
                    });
              } catch (Exception e) {
                handleError(new ErrorMessage(e, 400), context);
              }
            });
  }

  /**
   * This is an async callback that will be invoked whenever a request to /cluster is submited with
   * GET. Query a clusterIdOrName is provided in the format of /cluster/:clusterIdOrName, we will
   * fetch that specific id.
   *
   * <p>Example supported HTTP requests
   *
   * <p>GET http://iphere:porthere/cluster/ Will return all provisioned clusters
   *
   * <p>GET http://iphere:porthere/cluster/:id Will return the cluster with the provided id
   *
   * @param context RoutingContext Provided by vertx
   */
  private void getCluster(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                ObjectMapper om = ObjectMapperHolder.getMapper();
                StringBuilder response = new StringBuilder();
                String idOrNameToFetch = context.request().getParam("clusterIdOrName");
                if (idOrNameToFetch != null) {
                  Optional<Long> clusterId =
                      HttpUtils.getClusterIdFromIdOrName(server, idOrNameToFetch);
                  if (clusterId.isPresent()) {
                    BoundCluster cluster = server.getCluster(clusterId.get());
                    String clusterStr =
                        om.writerWithDefaultPrettyPrinter().writeValueAsString(cluster);
                    response.append(clusterStr);
                  } else {
                    handleError(
                        new ErrorMessage("No cluster registered with id " + idOrNameToFetch, 404),
                        context);
                    return;
                  }
                } else {
                  String clusterStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(server.getClusters());
                  response.append(clusterStr);
                }
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(200)
                    .end(response.toString());
              } catch (Exception e) {
                handleError(new ErrorMessage(e, 404), context);
              }
            });
  }

  /**
   * This method handles the registration of the various routes responsible for setting and
   * retrieving cluster information via http.
   *
   * @param router The router to register the endpoint with.
   */
  public void registerWithRouter(Router router) {
    router.route(HttpMethod.POST, "/cluster").handler(this::provisionCluster);
    router.route(HttpMethod.DELETE, "/cluster/:clusterIdOrName").handler(this::unregisterCluster);
    router.route(HttpMethod.DELETE, "/cluster").handler(this::unregisterCluster);
    router.route(HttpMethod.GET, "/cluster/:clusterIdOrName").handler(this::getCluster);
    router.route(HttpMethod.GET, "/cluster").handler(this::getCluster);
  }
}
