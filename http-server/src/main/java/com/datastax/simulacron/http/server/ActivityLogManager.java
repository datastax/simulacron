package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.List;
import java.util.stream.Collectors;

import static com.datastax.simulacron.http.server.HttpUtils.handleError;

public class ActivityLogManager implements HttpListener {
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
  private void getQueryLog(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                ObjectMapper om = ObjectMapperHolder.getMapper();
                StringBuilder response = new StringBuilder();
                String filterStr = context.request().getParam("filter");

                final Boolean filter =
                    filterStr != null ? filterStr.equalsIgnoreCase("primed") : null;

                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }

                HttpUtils.find(server, scope).getLogs();

                List<QueryLog> logs = HttpUtils.find(server, scope).getLogs();
                if (filter != null) {
                  logs =
                      logs.stream()
                          .filter(l -> l.isPrimed() == filter)
                          .collect(Collectors.toList());
                }
                String activityLogStr =
                    om.writerWithDefaultPrettyPrinter().writeValueAsString(logs);
                response.append(activityLogStr);
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
  private void deleteQueryLog(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              try {
                Scope scope = HttpUtils.getScope(context, server);
                if (scope == null) {
                  return;
                }

                HttpUtils.find(server, scope).clearLogs();
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(204)
                    .end();
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
