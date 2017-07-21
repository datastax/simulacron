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

import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.common.cluster.QueryLogReport;
import com.datastax.oss.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import static com.datastax.oss.simulacron.http.server.HttpUtils.handleError;

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
                QueryLogReport logReport;
                if (filter != null) logReport = HttpUtils.find(server, scope).getLogs(filter);
                else logReport = HttpUtils.find(server, scope).getLogs();

                ClusterQueryLogReport rootReport = logReport.getRootReport();
                String activityLogStr =
                    om.writerWithDefaultPrettyPrinter().writeValueAsString(rootReport);
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
