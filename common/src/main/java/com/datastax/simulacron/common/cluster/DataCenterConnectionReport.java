package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.Collection;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Represent a class that contains the connections of a particular datacenter. It's useful for
 * encoding the results with JSON.
 */
@JsonIgnoreProperties(value = {"name"})
public class DataCenterConnectionReport extends AbstractNodeProperties {
  @JsonManagedReference private final Collection<NodeConnectionReport> nodes = new TreeSet<>();

  @JsonBackReference private final ClusterConnectionReport parent;

  DataCenterConnectionReport() {
    // Default constructor for jackson deserialization.
    this(null, null);
  }

  DataCenterConnectionReport(Long id, ClusterConnectionReport clusterReport) {
    super(null, id, null, null, null);
    this.parent = clusterReport;
  }

  void addNode(NodeConnectionReport node) {
    assert node.getParent().orElse(null) == this;
    this.nodes.add(node);
  }

  public Collection<NodeConnectionReport> getNodes() {
    return nodes;
  }

  @Override
  @JsonIgnore
  public Long getActiveConnections() {
    return getNodes().stream().mapToLong(NodeConnectionReport::getActiveConnections).sum();
  }

  @Override
  public Optional<NodeProperties> getParent() {
    return Optional.ofNullable(parent);
  }

  @Override
  public Cluster getCluster() {
    return null;
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
}
