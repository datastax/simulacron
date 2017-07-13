package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.List;

/**
 * Represent a class that contains the connections of a particular node. It's useful for encoding
 * the results with JSON.
 */
@JsonIgnoreProperties(value = {"name"})
public class NodeConnectionReport extends ConnectionReport
    implements NodeStructure<ClusterConnectionReport, DataCenterConnectionReport> {
  @JsonProperty private final List<SocketAddress> connections;
  @JsonProperty private final SocketAddress address;

  @JsonBackReference private final DataCenterConnectionReport parent;

  @SuppressWarnings("unused")
  NodeConnectionReport() {
    // Default constructor for jackson deserialization.
    this(null, Collections.emptyList(), null, null);
  }

  public NodeConnectionReport(
      Long id,
      List<SocketAddress> connections,
      SocketAddress address,
      DataCenterConnectionReport parent) {
    super(id);
    this.connections = connections;
    this.address = address;
    this.parent = parent;
    if (parent != null) {
      parent.addNode(this);
    }
  }

  @Override
  public List<SocketAddress> getConnections() {
    return connections;
  }

  public SocketAddress getAddress() {
    return address;
  }

  @Override
  public DataCenterConnectionReport getDataCenter() {
    return parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeConnectionReport that = (NodeConnectionReport) o;
    if (!(this.getId().equals(that.getId()))) {
      return false;
    }
    return parent.equals(that.parent);
  }

  @Override
  public int hashCode() {
    return connections != null ? connections.hashCode() : 0;
  }

  @Override
  public ClusterConnectionReport getRootReport() {
    return getCluster();
  }
}
