package com.datastax.simulacron.common.cluster;

/**
 * Represents a subset of the whole domain of {@link Cluster}s. The granularity can go down to a
 * single {@link Cluster}, a single {@link DataCenter} or a single {@link Node}
 */
public class Scope {
  private final Long nodeId;
  private final Long dataCenterId;
  private final Long clusterId;

  /**
   * Defines scope for a given topic.
   *
   * @param topic Topic to find scope for.
   * @return scope for topic.
   */
  public static Scope scope(NodeProperties topic) {
    if (topic instanceof Cluster) {
      return new Scope((Cluster) topic);
    } else if (topic instanceof DataCenter) {
      return new Scope((DataCenter) topic);
    } else if (topic instanceof Node) {
      return new Scope((Node) topic);
    } else {
      throw new IllegalArgumentException("Unknown topic type for " + topic);
    }
  }

  /**
   * Defines scope for a node
   *
   * @param clusterId id of the cluster
   * @param dataCenterId: clusterId must be specified if dataCenterId is
   * @param nodeId: clusterId and dataCenterId must be specified if nodeId is
   */
  public Scope(Long clusterId, Long dataCenterId, Long nodeId) {
    this.nodeId = nodeId;
    this.dataCenterId = dataCenterId;
    this.clusterId = clusterId;
  }

  /**
   * Defines scope for a cluster
   *
   * @param cluster cluster to define scope for.
   */
  public Scope(Cluster cluster) {
    this(cluster.getId());
  }

  /**
   * Defines scope for a cluster
   *
   * @param clusterId id of the cluster
   */
  public Scope(Long clusterId) {
    this(clusterId, null, null);
  }

  /**
   * Defines scope for a data center
   *
   * @param datacenter data center to define scope for.
   */
  public Scope(DataCenter datacenter) {
    this(datacenter.getCluster().getId(), datacenter.getId());
  }

  /**
   * Defines scope for a data center
   *
   * @param clusterId id of the cluster
   * @param dataCenterId id of the datacenter
   */
  public Scope(Long clusterId, long dataCenterId) {
    this(clusterId, dataCenterId, null);
  }

  /**
   * Defines scope for a node
   *
   * @param node node to define scope for.
   */
  public Scope(Node node) {
    this(node.getCluster().getId(), node.getDataCenter().getId(), node.getId());
  }

  /**
   * @param node node to check scope for.
   * @return true if node is in this Scope, false otherwise
   */
  public Boolean isNodeInScope(Node node) {
    if ((this.nodeId == null) && (this.dataCenterId == null) && (this.clusterId == null)) {
      return true;
    }

    Boolean sameCluster = node.getCluster().getId().equals(clusterId);
    if (this.dataCenterId == null) {
      return sameCluster;
    }

    Boolean sameDatacenter = node.getDataCenter().getId().equals(dataCenterId);
    if (this.nodeId == null) {
      return sameCluster && sameDatacenter;
    }

    Boolean sameNode = node.getId().equals(nodeId);
    return sameCluster && sameDatacenter && sameNode;
  }

  /** @return true if the scope hasn't been specified, true otherwise */
  public Boolean isScopeUnSet() {
    return (nodeId == null && dataCenterId == null && clusterId == null);
  }

  public Long getNodeId() {
    return nodeId;
  }

  public Long getDataCenterId() {
    return dataCenterId;
  }

  public Long getClusterId() {
    return clusterId;
  }

  public String toString() {
    if (clusterId == null) {
      return "";
    }
    if (dataCenterId == null) {
      return nodeId.toString();
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
