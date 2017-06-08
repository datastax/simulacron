package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.datastax.simulacron.http.server.HttpUtils.handleError;

public class ActivityLogManager implements HttpListener {
  Logger logger = LoggerFactory.getLogger(ActivityLogManager.class);
  Server server;

  public ActivityLogManager(Server server) {
    this.server = server;
  }

  /**
   * This is an async callback that will be invoked whenever a request to /log is submitted with
   * GET. When a clusterIdOrName is provided in the format of /log/:clusterIdOrName, we will fetch
   * that specific id.
   *
   * <p>Example supported HTTP requests
   *
   * <p>GET http://iphere:porthere/log/:clusterIdOrName Will return all queries invoked by clients
   * to a cluster
   *
   * <p>GET http://iphere:porthere/log/:clusterIdOrName/:datacenterIdOrName Will return all queries
   * invoked by clients to a datacenter of a cluster
   *
   * <p>GET http://iphere:porthere/log/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName Will
   * return all queries invoked by clients to a node of a datacenter of a cluster
   *
   * @param context RoutingContext Provided by vertx
   */
  public void getQueryLog(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Map<Long, Cluster> clusters = this.server.getClusterRegistry();
                ObjectMapper om = ObjectMapperHolder.getMapper();
                StringBuilder response = new StringBuilder();
                String idToFetch = context.request().getParam("clusterIdOrName");
                String dcIdToFetch = context.request().getParam("datacenterIdOrName");
                String nodeIdToFetch = context.request().getParam("nodeIdOrName");
                String filterStr = context.request().getParam("filter");

                Boolean filter = null;
                if (filterStr != null) {
                  if (filterStr.equalsIgnoreCase("primed")) {
                    filter = true;
                  } else if (filterStr.equalsIgnoreCase("nonprimed")) {
                    filter = false;
                  }
                }

                Scope scope =
                    HttpUtils.parseQueryParameters(
                        idToFetch, dcIdToFetch, nodeIdToFetch, context, server);
                if (scope == null) {
                  return;
                }

                QueryLogScope queryLogScope = new QueryLogScope(scope, filter);
                if (scope.getClusterId() != null) {
                  Cluster cluster = clusters.get(scope.getClusterId());
                  List<QueryLog> logs = cluster.getActivityLog().getLogs(queryLogScope);
                  String activityLogStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(logs);
                  response.append(activityLogStr);
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
   * This is an async callback that will be invoked whenever a request to /log is submitted with
   * DELETE. When a clusterIdOrName is provided in the format of /log/:clusterIdOrName, we will
   * fetch that specific id.
   *
   * <p>Example supported HTTP requests
   *
   * <p>DELETE http://iphere:porthere/log/:clusterIdOrName Will delete all activity log invoked by
   * clients to a cluster
   *
   * <p>DELETE http://iphere:porthere/log/:clusterIdOrName/:datacenterIdOrName Will delete all
   * activity log invoked by clients to a datacenter of a cluster
   *
   * <p>DELETE http://iphere:porthere/log/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName Will
   * delete all activity log invoked by clients to a node of a datacenter of a cluster
   *
   * @param context RoutingContext Provided by vertx
   */
  public void deleteQueryLog(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Map<Long, Cluster> clusters = this.server.getClusterRegistry();
                ObjectMapper om = ObjectMapperHolder.getMapper();
                StringBuilder response = new StringBuilder();
                String idToFetch = context.request().getParam("clusterIdOrName");
                String dcIdToFetch = context.request().getParam("datacenterIdOrName");
                String nodeIdToFetch = context.request().getParam("nodeIdOrName");
                Scope scope =
                    HttpUtils.parseQueryParameters(
                        idToFetch, dcIdToFetch, nodeIdToFetch, context, server);
                if (scope == null) {
                  return;
                }

                QueryLogScope queryLogScope = new QueryLogScope(scope, null);
                if (scope.getClusterId() != null) {
                  Cluster cluster = clusters.get(scope.getClusterId());
                  cluster.getActivityLog().clearLogs(queryLogScope);
                }
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(204)
                    .end(response.toString());
              } catch (Exception e) {
                handleError(new ErrorMessage(e, 404), context);
              }
            });
  }

  public void registerWithRouter(Router router) {
    router.route(HttpMethod.GET, "/log/:clusterIdOrName").handler(this::getQueryLog);
    router
        .route(HttpMethod.GET, "/log/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::getQueryLog);
    router
        .route(HttpMethod.GET, "/log/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::getQueryLog);
    router.route(HttpMethod.DELETE, "/log/:clusterIdOrName").handler(this::deleteQueryLog);
    router
        .route(HttpMethod.DELETE, "/log/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::deleteQueryLog);
    router
        .route(HttpMethod.DELETE, "/log/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::deleteQueryLog);
  }
}
