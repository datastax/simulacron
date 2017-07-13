package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Represent a class that contains the connections of a particular datacenter. It's useful for
 * encoding the results with JSON.
 */
@JsonIgnoreProperties(value = {"name"})
public class DataCenterConnectionReport extends ConnectionReport
    implements DataCenterStructure<ClusterConnectionReport, NodeConnectionReport> {
  @JsonManagedReference private final Collection<NodeConnectionReport> nodes = new TreeSet<>();

  @JsonBackReference private final ClusterConnectionReport parent;

  @SuppressWarnings("unused")
  DataCenterConnectionReport() {
    // Default constructor for jackson deserialization.
    this(null, null);
  }

  public DataCenterConnectionReport(Long id, ClusterConnectionReport clusterReport) {
    super(id);
    this.parent = clusterReport;
    if (parent != null) {
      parent.addDataCenter(this);
    }
  }

  void addNode(NodeConnectionReport node) {
    assert node.getDataCenter() == this;
    this.nodes.add(node);
  }

  public Collection<NodeConnectionReport> getNodes() {
    return nodes;
  }

  @Override
  public ClusterConnectionReport getCluster() {
    return parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DataCenterConnectionReport that = (DataCenterConnectionReport) o;
    if (!(this.getId().equals(that.getId()))) {
      return false;
    }
    return parent.equals(that.parent);
  }

  @Override
  public int hashCode() {
    return nodes != null ? nodes.hashCode() : 0;
  }

  @Override
  public ClusterConnectionReport getRootReport() {
    return parent;
  }

  @Override
  public List<SocketAddress> getConnections() {
    return getNodes()
        .stream()
        .flatMap(n -> n.getConnections().stream())
        .collect(Collectors.toList());
  }
}
