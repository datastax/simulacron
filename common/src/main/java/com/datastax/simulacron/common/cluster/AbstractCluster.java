package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public abstract class AbstractCluster<D extends AbstractDataCenter<?, N>, N extends AbstractNode>
    extends AbstractNodeProperties implements ClusterStructure<D, N> {

  // json managed reference is used to indicate a two way linking between the 'parent' (cluster) and 'children'
  // (datacenters) in a json tree.  This tells the jackson mapping to tie child DCs to this cluster on deserialization.
  @JsonManagedReference
  @JsonProperty("data_centers")
  private final Collection<D> dataCenters = new ConcurrentSkipListSet<>();

  public AbstractCluster(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
  }

  @Override
  public Long getActiveConnections() {
    return dataCenters.stream().mapToLong(NodeProperties::getActiveConnections).sum();
  }

  @Override
  public Collection<D> getDataCenters() {
    return dataCenters;
  }

  @Override
  public Collection<N> getNodes() {
    return getDataCenters()
        .stream()
        .flatMap(dc -> dc.getNodes().stream())
        .collect(Collectors.toList());
  }

  /**
   * Intended to be called in {@link DataCenter} construction to add the {@link DataCenter} to this
   * cluster.
   *
   * @param dataCenter The data center to tie to this cluster.
   */
  void addDataCenter(D dataCenter) {
    assert dataCenter.getParent().orElse(null) == this;
    this.dataCenters.add(dataCenter);
  }

  @Override
  public String toString() {
    return toStringWith(
        ", dataCenters="
            + dataCenters.stream().map(d -> d.getId().toString()).collect(Collectors.joining(",")));
  }

  @Override
  @JsonIgnore
  public Optional<NodeProperties> getParent() {
    return Optional.empty();
  }
}
