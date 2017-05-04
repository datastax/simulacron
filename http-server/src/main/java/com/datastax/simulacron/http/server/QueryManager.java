package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.simulacron.http.server.HttpUtils.handleError;

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
  public void primerQuery(RoutingContext context) {

    context
        .request()
        .bodyHandler(
            totalBuffer -> {
              String jsonBody = "";
              try {
                System.out.println("Full body received, length = " + totalBuffer.length());
                jsonBody = totalBuffer.toString();
                ObjectMapper om = ObjectMapperHolder.getMapper();
                QueryPrime query = om.readValue(jsonBody, QueryPrime.class);

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

                server.registerStub(new QueryHandler(query));
              } catch (Exception e) {
                handleQueryError(e, "prime query", context);
              }
              if (!context.response().ended()) {
                context.request().response().setStatusCode(201).end(jsonBody);
              }
              ;
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
              try {
                server.clearStubsMatchingType(QueryHandler.class);
              } catch (Exception e) {
                handleQueryError(e, "clear primed queries", context);
              }
              if (!context.response().ended()) {
                context.request().response().setStatusCode(200).end(jsonBody);
              }
              ;
            });
  }

  /**
   * This is an async callback that will be invoked whenever a request to /log is submitted with
   * GET. When a clusterId is provided in the format of /log/:clusterId, we will fetch that specific
   * id.
   *
   * <p>Example supported HTTP requests
   *
   * <p>GET http://iphere:porthere/log/:clusterId Will return all queries invoked by clients to a
   * cluster
   *
   * <p>GET http://iphere:porthere/log/:clusterId/:datacenterId Will return all queries invoked by
   * clients to a datacenter of a cluster
   *
   * <p>GET http://iphere:porthere/log/:clusterIdi/:datacenterId/:nodeId Will return all queries
   * invoked by clients to a node of a datacenter of a cluster
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
                String idToFetch = context.request().getParam("clusterId");
                String dcIdToFetch = context.request().getParam("datacenterId");
                String nodeIdToFetch = context.request().getParam("nodeId");
                if (idToFetch != null) {
                  Long id = Long.parseLong(idToFetch);
                  Cluster cluster = clusters.get(id);
                  if (cluster == null) {
                    handleError(
                        new ErrorMessage("No cluster registered with id " + id, 404), context);
                  }

                  List<QueryLog> logs = cluster.getActivityLog().getLogs();

                  if (dcIdToFetch != null && nodeIdToFetch != null) {
                    try {
                      long datacenterId = Long.parseLong(dcIdToFetch);
                      long nodeId = Long.parseLong(nodeIdToFetch);
                      logs = cluster.getActivityLog().getLogsFromNode(datacenterId, nodeId);
                    } catch (NumberFormatException exception) {
                      handleError(new ErrorMessage(exception.getMessage(), 404), context);
                    }
                  }

                  if (dcIdToFetch != null && nodeIdToFetch == null) {
                    try {
                      long datacenterId = Long.parseLong(dcIdToFetch);
                      logs = cluster.getActivityLog().getLogsFromDatacenter(datacenterId);
                    } catch (NumberFormatException exception) {
                      handleError(new ErrorMessage(exception.getMessage(), 404), context);
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
    router.route(HttpMethod.POST, "/prime*").handler(this::primerQuery);
    router.route(HttpMethod.DELETE, "/prime*").handler(this::clearPrimedQueries);
    router.route(HttpMethod.GET, "/log/:clusterId").handler(this::getQueryLog);
    router.route(HttpMethod.GET, "/log/:clusterId/:datacenterId").handler(this::getQueryLog);
    router
        .route(HttpMethod.GET, "/log/:clusterId/:datacenterId/:nodeId")
        .handler(this::getQueryLog);
  }
}
