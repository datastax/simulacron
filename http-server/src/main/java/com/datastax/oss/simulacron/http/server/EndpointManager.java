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

import com.datastax.oss.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundTopic;
import com.datastax.oss.simulacron.server.RejectScope;
import com.datastax.oss.simulacron.server.Server;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.net.InetSocketAddress;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointManager implements HttpListener {
  private final Logger logger = LoggerFactory.getLogger(EndpointManager.class);
  private final Server server;
  private final ObjectMapper om = ObjectMapperHolder.getMapper();

  public EndpointManager(Server server) {
    this.server = server;
  }

  /**
   * This is an async callback that will be invoked in order to get the existing connections of a
   * particular scope or the whole of the connections that have been created by simulacron (all the
   * connections of all the clusters) if the scope is not set. The scope will be specified in a
   * similar way to other requests, adding /clusterIdOrName/datacenterIdOrName/nodeIdOrName to the
   * path
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>GET http://iphere:porthere/connections/clusterIdOrName/datacenterIdOrName/ this will
   * retrieve all the connections of the datacenter represented by datacenterIdOrName inside the
   * cluster represented by clusterIdOrName
   *
   * <p>GET http://iphere:porthere/connections/ this will retrieve all the connections
   *
   * @param context RoutingContext provided by vertx
   */
  private void getConnections(RoutingContext context) {
    setResponseFromReport(context, t -> t.getConnections().getRootReport());
  }

  /**
   * This is an async callback that will be invoked in order to close the exisiting connections of a
   * particular scope or the whole of the connections that have been created by simulacron (all the
   * connections of all the clusters) if the scope is not set. The scope will be specified in a
   * similar way to other requests, adding /clusterIdOrName/datacenterIdOrName/nodeIdOrName to the
   * path. It also accepts a parameter type that specifies how to close the information. More
   * information in {@link CloseType}.
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>DELETE http://iphere:porthere/connections/clusterIdOrName/datacenterIdOrName/ this will
   * close all the connections of the datacenter represented by datacenterIdOrName inside the
   * cluster represented by clusterIdOrName
   *
   * <p>DELETE http://iphere:porthere/connections?type=shutdown_read this will delete all the
   * connections
   *
   * @param context RoutingContext provided by vertx
   */
  private void closeConnections(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String type = context.request().getParam("type");
                if (type == null) {
                  type = "disconnect";
                }

                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }
                CloseType closeType = CloseType.valueOf(type.toUpperCase());
                CompletionStage<? extends ConnectionReport> reportFuture =
                    HttpUtils.find(server, scope).closeConnectionsAsync(closeType);
                StringBuilder response = new StringBuilder();

                reportFuture.whenComplete(
                    (report, ex) -> {
                      if (ex == null) {
                        try {
                          String reportStr =
                              om.writerWithDefaultPrettyPrinter()
                                  .writeValueAsString(report.getRootReport());
                          response.append(reportStr);
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
                            .setStatusCode(200)
                            .end(response.toString());
                      }
                    });
              } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error ocurred while processing closeConnections request");
                handleError(new ErrorMessage(e.getMessage(), 400), context);
              }
            });
  }

  /**
   * This is an async callback that will be invoked in order to close only one connection. It can be
   * useful when the granularity is smaller than one node and it's not convenient to use {@link
   * #closeConnections(RoutingContext)}
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>DELETE http://iphere:porthere/connections/ip/port?type=disconnect this will close the
   * connection with origin that ip and that port
   *
   * @param context RoutingContext provided by vertx
   */
  private void closeConnectionByIp(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String clusterIdOrName = context.request().getParam("clusterIdOrName");
                Optional<Long> clusterId =
                    HttpUtils.getClusterIdFromIdOrName(server, clusterIdOrName);
                if (!clusterId.isPresent()) {
                  handleError(
                      new ErrorMessage("No cluster registered with id " + clusterIdOrName, 404),
                      context);
                  return;
                }

                BoundCluster cluster = server.getCluster(clusterId.get());
                String ip = context.request().getParam("ip");
                String portS = context.request().getParam("port");
                Integer port = Integer.parseInt(portS);

                String type = context.request().getParam("type");
                if (type == null) {
                  type = "disconnect";
                }

                InetSocketAddress connection = new InetSocketAddress(ip, port);
                CompletionStage<ClusterConnectionReport> reportFuture =
                    cluster.closeConnectionAsync(connection, CloseType.valueOf(type.toUpperCase()));
                StringBuilder response = new StringBuilder();

                reportFuture.whenComplete(
                    (clusterReport, ex) -> {
                      if (ex == null) {
                        try {
                          String reportStr =
                              om.writerWithDefaultPrettyPrinter().writeValueAsString(clusterReport);
                          response.append(reportStr);
                          context
                              .request()
                              .response()
                              .putHeader("content-type", "application/json")
                              .setStatusCode(200)
                              .end(response.toString());
                        } catch (JsonProcessingException jpex) {
                          logger.error(
                              "Error encountered when attempting to form json response", jpex);
                        }
                      } else {
                        int statusCode = 400;
                        if (ex instanceof IllegalArgumentException) {
                          statusCode = 404;
                        }
                        handleError(new ErrorMessage(ex.getMessage(), statusCode), context);
                      }
                    });
              } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error occurred while processing closeConnections request");
                handleError(new ErrorMessage(e.getMessage(), 400), context);
              }
            });
  }

  /**
   * This is an async callback that will be invoked in order to reject the connection attemps to a
   * particular scope or all the clusters if the scope is not set. The scope will be specified in a
   * similar way to other requests, adding /clusterIdOrName/datacenterIdOrName/nodeIdOrName to the
   * path. It accepts a parameter type that specifies how to reject the future connections. More
   * information in {@link RejectScope}. The parameter after specifies after how many attemps will
   * the rejection be effective
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>DELETE http://iphere:porthere/connections/listener/clusterIdOrName/datacenterIdOrName/ this
   * will reject all the connections to the datacenter represented by datacenterIdOrName inside the
   * cluster represented by clusterIdOrName
   *
   * <p>DELETE http://iphere:porthere/connections/listener?type=unbind?after=3 this will unbind all
   * the channels of all nodes after three connection attemps to that node
   *
   * @param context RoutingContext provided by vertx
   */
  private void rejectConnections(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                String afterS = context.request().getParam("after");
                String type = context.request().getParam("type");
                Integer after = afterS == null ? 0 : Integer.parseInt(afterS);

                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }
                RejectScope rejectScope = RejectScope.valueOf(type.toUpperCase());
                CompletionStage<Void> future =
                    HttpUtils.find(server, scope).rejectConnectionsAsync(after, rejectScope);
                future.whenComplete(
                    (completedCluster, ex) -> {
                      if (ex == null) {
                        context
                            .request()
                            .response()
                            .putHeader("content-type", "application/json")
                            .setStatusCode(200)
                            .end();
                      } else {
                        handleError(new ErrorMessage(ex.getMessage(), 400), context);
                      }
                    });
              } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error ocurred while processing closeConnections request");
                handleError(new ErrorMessage(e.getMessage(), 400), context);
              }
            });
  }

  /**
   * This is an async callback that will be invoked in order to accept connections to a particular
   * scope or all the clusters if the scope is not set. The scope will be specified in a similar way
   * to other requests, adding /clusterIdOrName/datacenterIdOrName/nodeIdOrName to the path. It will
   * usually be called after {@link #rejectConnections(RoutingContext)}.
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>PUT http://iphere:porthere/connections/listener/clusterIdOrName/datacenterIdOrName/ this
   * will accept again the connections to the datacenter represented by datacenterIdOrName inside
   * the cluster represented by clusterIdOrName if {@link #rejectConnections(RoutingContext)} had
   * been called on this nodes before, otherwise, nothing will be done
   *
   * @param context RoutingContext provided by vertx
   */
  private void acceptConnections(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }
                CompletionStage<Void> future =
                    HttpUtils.find(server, scope).acceptConnectionsAsync();
                future.whenComplete(
                    (completedCluster, ex) -> {
                      if (ex == null) {
                        context
                            .request()
                            .response()
                            .putHeader("content-type", "application/json")
                            .setStatusCode(200)
                            .end();
                      } else {
                        handleError(new ErrorMessage(ex.getMessage(), 400), context);
                      }
                    });
              } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error ocurred while processing closeConnections request");
                handleError(new ErrorMessage(e.getMessage(), 400), context);
              }
            });
  }

  private void pauseConnections(RoutingContext context) {
    setResponseFromReport(context, t -> t.pauseRead().getRootReport());
  }

  private void resumeConnections(RoutingContext context) {
    setResponseFromReport(context, t -> t.resumeRead().getRootReport());
  }

  /** Sets the response based using a sync handler */
  private void setResponseFromReport(
      RoutingContext context, Function<BoundTopic<?, ?>, ConnectionReport> topicHandler) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }

                // Use topicHandler to obtain the report
                ConnectionReport report = topicHandler.apply(HttpUtils.find(server, scope));
                StringBuilder response = new StringBuilder();

                String connectionsStr =
                    om.writerWithDefaultPrettyPrinter().writeValueAsString(report);
                response.append(connectionsStr);
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(200)
                    .end(response.toString());
              } catch (Exception e) {
                logger.error("Error occurred while processing getConnections request", e);
                handleError(new ErrorMessage(e.getMessage(), 400), context);
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
    // Get connections
    router
        .route(HttpMethod.GET, "/connections/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::getConnections);
    router
        .route(HttpMethod.GET, "/connections/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::getConnections);
    router.route(HttpMethod.GET, "/connections/:clusterIdOrName").handler(this::getConnections);

    // Delete connections
    router
        .route(HttpMethod.DELETE, "/connections/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::closeConnections);
    router
        .route(HttpMethod.DELETE, "/connections/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::closeConnections);
    router
        .route(HttpMethod.DELETE, "/connections/:clusterIdOrName")
        .handler(this::closeConnections);
    router
        .route(HttpMethod.DELETE, "/connection/:clusterIdOrName/:ip/:port")
        .handler(this::closeConnectionByIp);

    // Stop listening for connections
    router
        .route(HttpMethod.DELETE, "/listener/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::rejectConnections);
    router
        .route(HttpMethod.DELETE, "/listener/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::rejectConnections);
    router.route(HttpMethod.DELETE, "/listener/:clusterIdOrName").handler(this::rejectConnections);

    // Restore listening for connections
    router
        .route(HttpMethod.PUT, "/listener/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::acceptConnections);
    router
        .route(HttpMethod.PUT, "/listener/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::acceptConnections);
    router.route(HttpMethod.PUT, "/listener/:clusterIdOrName").handler(this::acceptConnections);

    // Pause reads for connections
    router
        .route(HttpMethod.PUT, "/pause-reads/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::pauseConnections);
    router
        .route(HttpMethod.PUT, "/pause-reads/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::pauseConnections);
    router
        .route(HttpMethod.PUT, "/pause-reads/:clusterIdOrName")
        .handler(this::pauseConnections);

    // Resume reads for connections
    router
        .route(HttpMethod.DELETE, "/pause-reads/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::resumeConnections);
    router
        .route(HttpMethod.DELETE, "/pause-reads/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::resumeConnections);
    router
        .route(HttpMethod.DELETE, "/pause-reads/:clusterIdOrName")
        .handler(this::resumeConnections);
  }
}
