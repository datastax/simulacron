package com.datastax.simulacron.common.cluster;

/**
 * Represents a subset of the whole domain of {@link Cluster}s. The granularity can go down to a
 * single {@link Cluster}, a single {@link DataCenter} or a single {@link Node}
 */
public class Scope {
  Long nodeId = null;
  Long datacenterId = null;
  Long clusterId = null;

  /**
   * @param clusterId
   * @param datacenterId: clusterId myust be specified is datacenterId is
   * @param nodeId: clusterId and datacenterId must be specified is nodeId is
   */
  public Scope(Long clusterId, Long datacenterId, Long nodeId) {
    this.nodeId = nodeId;
    this.datacenterId = datacenterId;
    this.clusterId = clusterId;
  }

  /**
   * @param node
   * @return true if node is in this Scope, false otherwise
   */
  public Boolean isNodeInScope(Node node) {
    if ((this.nodeId == null) && (this.datacenterId == null) && (this.clusterId == null)) {
      return true;
    }

    Boolean sameCluster = node.getCluster().getId().equals(clusterId);
    if (this.datacenterId == null) {
      return sameCluster;
    }

    Boolean sameDatacenter = node.getDataCenter().getId().equals(datacenterId);
    if (this.nodeId == null) {
      return sameCluster && sameDatacenter;
    }

    Boolean sameNode = node.getId().equals(nodeId);
    return sameCluster && sameDatacenter && sameNode;
  }

  /** @return true if the scope hasn't been specified, true otherwise */
  public Boolean isScopeUnSet() {
    return (nodeId == null && datacenterId == null && clusterId == null);
  }

  public Long getNodeId() {
    return nodeId;
  }

  public Long getDatacenterId() {
    return datacenterId;
  }

  public Long getClusterId() {
    return clusterId;
  }

  public String toString() {
    if (clusterId == null) {
      return "";
    }
    if (datacenterId == null) {
      return nodeId.toString();
    }
    if (nodeId == null) {
      return clusterId.toString() + "/" + datacenterId.toString();
    }
    return clusterId.toString() + "/" + datacenterId.toString() + "/" + nodeId.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Scope scope = (Scope) o;

    if (nodeId != null ? !nodeId.equals(scope.nodeId) : scope.nodeId != null) return false;
    if (datacenterId != null
        ? !datacenterId.equals(scope.datacenterId)
        : scope.datacenterId != null) return false;
    return clusterId != null ? clusterId.equals(scope.clusterId) : scope.clusterId == null;
  }

  @Override
  public int hashCode() {
    int result = nodeId != null ? nodeId.hashCode() : 0;
    result = 31 * result + (datacenterId != null ? datacenterId.hashCode() : 0);
    result = 31 * result + (clusterId != null ? clusterId.hashCode() : 0);
    return result;
  }
}
