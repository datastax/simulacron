package com.datastax.simulacron.http.server;

/**
 * Represents a subset of the whole domain of cluster. The granularity can go down to a single
 * cluster, data center or node.
 */
class Scope {
  private final Long nodeId;
  private final Long dataCenterId;
  private final Long clusterId;

  /**
   * Defines scope for a node
   *
   * @param clusterId id of the cluster
   * @param dataCenterId: clusterId must be specified if dataCenterId is
   * @param nodeId: clusterId and dataCenterId must be specified if nodeId is
   */
  Scope(Long clusterId, Long dataCenterId, Long nodeId) {
    this.nodeId = nodeId;
    this.dataCenterId = dataCenterId;
    this.clusterId = clusterId;
  }

  Long getNodeId() {
    return nodeId;
  }

  Long getDataCenterId() {
    return dataCenterId;
  }

  Long getClusterId() {
    return clusterId;
  }

  public String toString() {
    if (clusterId == null) {
      return "";
    }
    if (dataCenterId == null) {
      return clusterId.toString();
    }
    if (nodeId == null) {
      return clusterId.toString() + "/" + dataCenterId.toString();
    }
    return clusterId.toString() + "/" + dataCenterId.toString() + "/" + nodeId.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Scope scope = (Scope) o;

    return (nodeId != null ? nodeId.equals(scope.nodeId) : scope.nodeId == null)
        && (dataCenterId != null
            ? dataCenterId.equals(scope.dataCenterId)
            : scope.dataCenterId == null)
        && (clusterId != null ? clusterId.equals(scope.clusterId) : scope.clusterId == null);
  }

  @Override
  public int hashCode() {
    int result = nodeId != null ? nodeId.hashCode() : 0;
    result = 31 * result + (dataCenterId != null ? dataCenterId.hashCode() : 0);
    result = 31 * result + (clusterId != null ? clusterId.hashCode() : 0);
    return result;
  }
}
