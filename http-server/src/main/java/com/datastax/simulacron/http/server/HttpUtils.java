package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.AbstractNodeProperties;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.server.BoundCluster;
import com.datastax.simulacron.server.BoundDataCenter;
import com.datastax.simulacron.server.BoundTopic;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

class HttpUtils {
  private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);
  private static final ObjectMapper om = ObjectMapperHolder.getMapper();

  static void handleError(ErrorMessage message, RoutingContext context) {
    if (message.getException() != null) {
      logger.error(message.getMessage(), message.getException());
    }
    handleMessage(message, context);
  }

  static void handleMessage(Message message, RoutingContext context) {
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

  static Scope parseQueryParameters(
      String idToFetchS,
      String dcIdToFetchS,
      String nodeIdToFetchS,
      RoutingContext context,
      Server server) {
    Long idToFetch = null;
    Long dcIdToFetch = null;
    Long nodeIdToFetch = null;

    if (idToFetchS != null) {
      Optional<Long> clusterId = getClusterIdFromIdOrName(server, idToFetchS);
      if (clusterId.isPresent()) {
        idToFetch = clusterId.get();

        if (dcIdToFetchS != null) {
          Optional<Long> dcId = getDatacenterIdFromIdOrName(server, idToFetch, dcIdToFetchS);
          if (dcId.isPresent()) {
            dcIdToFetch = dcId.get();

            if (nodeIdToFetchS != null) {
              Optional<Long> nodeId =
                  getNodeIdFromIdOrName(server, idToFetch, dcIdToFetch, nodeIdToFetchS);

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

  static Scope getScope(RoutingContext context, Server server) {
    String idToFetchS = context.request().getParam("clusterIdOrName");
    String dcIdToFetchS = context.request().getParam("datacenterIdOrName");
    String nodeIdToFetchS = context.request().getParam("nodeIdOrName");
    return HttpUtils.parseQueryParameters(
        idToFetchS, dcIdToFetchS, nodeIdToFetchS, context, server);
  }

  /**
   * Convenience method for retrieving the id of a cluster given the id or the name of the cluster
   *
   * @param idOrName string which could be the id or the name of the cluster
   * @return Id of the cluster
   */
  static Optional<Long> getClusterIdFromIdOrName(Server server, String idOrName) {
    try {
      long id = Long.parseLong(idOrName);
      if (server.getCluster(id) != null) {
        return Optional.of(id);
      } else {
        return Optional.empty();
      }
    } catch (NumberFormatException e) {
      return server
          .getClusters()
          .stream()
          .filter(c -> c.getName().equals(idOrName))
          .findAny()
          .map(AbstractNodeProperties::getId);
    }
  }

  /**
   * Convenience method for retrieving the id of a datacenter given the id or the name of the
   * datacenter, and the id of the cluster it belongs to
   *
   * @param clusterId id of the cluster the datacenter belongs to
   * @param idOrName string which could be the id or the name of the DataCenter
   * @return id of the datacenter
   */
  static Optional<Long> getDatacenterIdFromIdOrName(
      Server server, Long clusterId, String idOrName) {
    return server
        .getCluster(clusterId)
        .getDataCenters()
        .stream()
        .filter(d -> d.getName().equals(idOrName) || d.getId().toString().equals(idOrName))
        .findAny()
        .map(AbstractNodeProperties::getId);
  }

  /**
   * Convenience method for retrieving the id of a Node given the id or the name of the node, and
   * the id of the cluster and the datacenter it belongs to
   *
   * @param clusterId id of the cluster the DataCenter belongs to
   * @param datacenterId id of the datacenter the DataCenter belongs to
   * @param idOrName string which could be the id or the name of the Node
   * @return id of the cluster
   */
  static Optional<Long> getNodeIdFromIdOrName(
      Server server, Long clusterId, Long datacenterId, String idOrName) {
    Optional<DataCenter> dc =
        server
            .getCluster(clusterId)
            .getDataCenters()
            .stream()
            .filter(d -> d.getId().equals(datacenterId))
            .findAny();

    return dc.flatMap(
        d ->
            d.getNodes()
                .stream()
                .filter(n -> n.getName().equals(idOrName) || n.getId().toString().equals(idOrName))
                .findAny()
                .map(AbstractNodeProperties::getId));
  }

  /**
   * Resolve the cluster, dc or node for the given scope. Scope must be set at least at the cluster
   * level.
   *
   * @param scope Scope to resolve from.
   * @return Resolved topic.
   */
  static BoundTopic<?> find(Server server, Scope scope) {
    if (scope.getClusterId() != null) {
      BoundCluster cluster = server.getCluster(scope.getClusterId());
      if (scope.getDataCenterId() != null) {
        BoundDataCenter dc = cluster.dc(scope.getDataCenterId());
        if (scope.getNodeId() != null) {
          return cluster.node(dc.getId(), scope.getNodeId());
        } else {
          return dc;
        }
      } else {
        return cluster;
      }
    } else {
      throw new IllegalArgumentException("Scope must include cluster id");
    }
  }
}
