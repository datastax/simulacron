package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.datastax.simulacron.http.server.HttpUtils.handleError;
import static com.datastax.simulacron.http.server.HttpUtils.handleMessage;

public class QueryManager implements HttpListener {
  Logger logger = LoggerFactory.getLogger(QueryManager.class);
  Server server;

  public QueryManager(Server server) {
    this.server = server;
  }

  /**
   * This is an async callback that will be invoked whenever a request to /prime is posted. It will
   * parse the provided json and construct a query which when called from the server will return the
   * rows defined.
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>POST http://iphere:porthere/prime-query-single with json body of {@code { "when": { "query":
   * "SELECT * FROM table" }, "then": { "rows": [ { "row1": "sample1", "row2": "1" } ], "result":
   * "success", "column_types": { "row1": "ascii", "row2": "bigint" } } }}
   *
   * <p>This will return a row containing two columns when a SELECT * FROM table. The row will
   * contain two fields row1, and row2, that are of type ascii and bigint respectively
   *
   * @param context RoutingContext provided by vertx
   */
  public void primeQuery(RoutingContext context) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              String jsonBody = "";
              try {
                logger.info("Full body received, length = " + totalBuffer.length());

                String idToFetchS = context.request().getParam("clusterIdOrName");
                String dcIdToFetchS = context.request().getParam("datacenterIdOrName");
                String nodeIdToFetchS = context.request().getParam("nodeIdOrName");

                Scope scope =
                    this.parseQueryParameters(idToFetchS, dcIdToFetchS, nodeIdToFetchS, context);
                if (scope == null) {
                  return;
                }

                jsonBody = totalBuffer.toString();
                ObjectMapper om = ObjectMapperHolder.getMapper();
                RequestPrime query = om.readValue(jsonBody, RequestPrime.class);

                if (query.then instanceof SuccessResult) {
                  SuccessResult success = (SuccessResult) query.then;
                  for (String key : success.columnTypes.keySet()) {
                    String typeName = success.columnTypes.get(key);
                    RawType type = CodecUtils.getTypeFromName(typeName);
                    if (type == null) {
                      handleBadType(key, typeName, context);
                    }
                  }
                }
                RequestHandler requestHandler = new RequestHandler(query);
                requestHandler.setScope(scope);

                server.registerStub(requestHandler);
              } catch (Exception e) {
                handleQueryError(e, "prime query", context);
              }
              if (!context.response().ended()) {
                context
                    .request()
                    .response()
                    .putHeader("content-type", "application/json")
                    .setStatusCode(201)
                    .end(jsonBody);
              }
            });
  }
  /**
   * This is an async callback that will be invoked whenever a request to /clear_primed is posted.
   * It will remove all primed queries that have been set so far
   *
   * <p>Example Supported HTTP Requests
   *
   * <p>POST http://iphere:porthere/clear_primed no payload.
   *
   * @param context RoutingContext provided by vertx
   */
  public void clearPrimedQueries(RoutingContext context) {
    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              String jsonBody = "";

              String idToFetchS = context.request().getParam("clusterId");
              String dcIdToFetchS = context.request().getParam("datacenterId");
              String nodeIdToFetchS = context.request().getParam("nodeId");

              Scope scope =
                  this.parseQueryParameters(idToFetchS, dcIdToFetchS, nodeIdToFetchS, context);
              if (scope == null) {
                return;
              }

              int cleared = 0;
              try {
                server.clear(scope, RequestHandler.class);
              } catch (Exception e) {
                handleQueryError(e, "clear primed queries", context);
              }
              if (!context.response().ended()) {
                handleMessage(new Message("Cleared " + cleared + " primed queries", 202), context);
              }
            });
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
                Map<Long, Cluster> clusters = this.server.getClusterRegistry();
                ObjectMapper om = ObjectMapperHolder.getMapper();
                StringBuilder response = new StringBuilder();
                String idToFetchS = context.request().getParam("clusterIdOrName");
                String dcIdToFetchS = context.request().getParam("datacenterIdOrName");
                String nodeIdToFetchS = context.request().getParam("nodeIdOrName");

                Scope scope =
                    this.parseQueryParameters(idToFetchS, dcIdToFetchS, nodeIdToFetchS, context);

                if (scope == null) {
                  return;
                }

                if (scope.getClusterId() != null) {
                  Cluster cluster = clusters.get(scope.getClusterId());
                  List<QueryLog> logs = cluster.getActivityLog().getLogs();
                  if (scope.getDatacenterId() != null) {
                    Long dcId = scope.getDatacenterId();
                    if (scope.getNodeId() != null) {
                      logs = cluster.getActivityLog().getLogsFromNode(dcId, scope.getNodeId());
                    } else {
                      logs = cluster.getActivityLog().getLogsFromDatacenter(dcId);
                    }
                  }

                  String activityLogStr =
                      om.writerWithDefaultPrettyPrinter().writeValueAsString(logs);
                  response.append(activityLogStr);
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
                handleError(new ErrorMessage(e, 404), context);
              }
            });
  }

  private Scope parseQueryParameters(
      String idToFetchS, String dcIdToFetchS, String nodeIdToFetchS, RoutingContext context) {
    Long idToFetch = null;
    Long dcIdToFetch = null;
    Long nodeIdToFetch = null;

    if (idToFetchS != null) {
      Optional<Long> clusterId = server.getClusterIdFromIdOrName(idToFetchS);
      if (clusterId.isPresent()) {
        idToFetch = clusterId.get();

        if (dcIdToFetchS != null) {
          Optional<Long> dcId = server.getDatacenterIdFromIdOrName(idToFetch, dcIdToFetchS);
          if (dcId.isPresent()) {
            dcIdToFetch = dcId.get();

            if (nodeIdToFetchS != null) {
              Optional<Long> nodeId =
                  server.getNodeIdFromIdOrName(idToFetch, dcIdToFetch, nodeIdToFetchS);

              if (nodeId.isPresent()) {
                nodeIdToFetch = nodeId.get();
              } else {
                handleError(
                    new ErrorMessage(
                        "No node registered with id "
                            + idToFetch
                            + "/"
                            + dcIdToFetch
                            + "/"
                            + nodeIdToFetchS,
                        404),
                    context);
                return null;
              }
            }
          } else {
            handleError(
                new ErrorMessage(
                    "No datacenter registered with id " + idToFetchS + "/" + dcIdToFetchS, 404),
                context);
            return null;
          }
        }
      } else {
        handleError(new ErrorMessage("No cluster registered with id " + idToFetchS, 404), context);
        return null;
      }
    }
    return new Scope(idToFetch, dcIdToFetch, nodeIdToFetch);
  }

  /**
   * Convenience method to set failure on response and print a relative error messaged
   *
   * @param e Exception thrown
   * @param operation for logging.
   * @param context RoutingContext to set failure upon
   */
  private void handleQueryError(Throwable e, String operation, RoutingContext context) {
    String errorString =
        "Error encountered while attempting to " + operation + ". See logs for details";
    HttpUtils.handleError(new ErrorMessage(errorString, 400, e), context);
  }
  /**
   * Convenience method to set failure on response when an invalid type is detected
   *
   * @param context RoutingContext to set failure upon
   */
  private void handleBadType(String key, String typeName, RoutingContext context) {
    String errorMsg =
        "Invalid type defined for column " + key + ", " + typeName + " is not a recognized type.";
    HttpUtils.handleError(new ErrorMessage(errorMsg, 400), context);
  }

  public void registerWithRouter(Router router) {
    // Priming queries
    router.route(HttpMethod.POST, "/prime-query-single/:clusterIdOrName").handler(this::primeQuery);
    router
        .route(HttpMethod.POST, "/prime-query-single/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::primeQuery);
    router
        .route(
            HttpMethod.POST,
            "/prime-query-single/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::primeQuery);

    router.route(HttpMethod.POST, "/prime*").handler(this::primeQuery);

    //Deleting primed queries
    router
        .route(HttpMethod.DELETE, "/prime-query-single/:clusterIdOrName")
        .handler(this::clearPrimedQueries);
    router
        .route(HttpMethod.DELETE, "/prime-query-single/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::clearPrimedQueries);
    router
        .route(
            HttpMethod.DELETE,
            "/prime-query-single/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::primeQuery);
    router.route(HttpMethod.DELETE, "/prime*").handler(this::clearPrimedQueries);

    //Logging queries
    router.route(HttpMethod.GET, "/log/:clusterIdOrName").handler(this::getQueryLog);
    router
        .route(HttpMethod.GET, "/log/:clusterIdOrName/:datacenterIdOrName")
        .handler(this::getQueryLog);
    router
        .route(HttpMethod.GET, "/log/:clusterIdOrName/:datacenterIdOrName/:nodeIdOrName")
        .handler(this::getQueryLog);
  }
}
