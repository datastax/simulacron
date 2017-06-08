package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.common.cluster.Scope;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  private static final ObjectMapper om = ObjectMapperHolder.getMapper();

  public static void handleError(ErrorMessage message, RoutingContext context) {
    if (message.getException() != null) {
      logger.error(message.getMessage(), message.getException());
    }
    handleMessage(message, context);
  }

  public static void handleMessage(Message message, RoutingContext context) {
    try {
      String msgJson = om.writerWithDefaultPrettyPrinter().writeValueAsString(message);
      context
          .request()
          .response()
          .putHeader("content-type", "application/json")
          .setStatusCode(message.getStatusCode())
          .end(msgJson);
    } catch (JsonProcessingException e) {
      context
          .request()
          .response()
          .putHeader("content-type", "application/json")
          .setStatusCode(500)
          .end("{\"message\": \"Internal Server Error, refer to logs\", \"status_code\": 500}");
    }
  }

  public static Scope parseQueryParameters(
      String idToFetchS,
      String dcIdToFetchS,
      String nodeIdToFetchS,
      RoutingContext context,
      Server server) {
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
}
