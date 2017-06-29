package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Represent a class that contains the connections of a particular cluster. It's useful for encoding
 * the results with JSON.
 */
@JsonIgnoreProperties(value = {"name"})
public class ClusterConnectionReport extends AbstractNodeProperties {

  @JsonManagedReference
  @JsonProperty("data_centers")
  private final Collection<DataCenterConnectionReport> dataCenters = new TreeSet<>();

  ClusterConnectionReport() {
    // Default constructor for jackson deserialization.
    this(null);
  }

  public ClusterConnectionReport(Long id) {
    super(null, id, null, null, null);
  }

  /**
   * Convenience method for adding a single node's connection to ClusterConnectionReport
   *
   * @param node Node to add
   * @param addressList client side of the connections this node has
   * @param serverAddress the address where this node is listening
   */
  public void addNode(Node node, List<SocketAddress> addressList, SocketAddress serverAddress) {
    Long id = node.getParent().get().getId();
    Optional<DataCenterConnectionReport> optionalDatacenterReport =
        dataCenters.stream().filter(dc -> dc.getId().equals(id)).findFirst();
    DataCenterConnectionReport datacenterReport;
    if (optionalDatacenterReport.isPresent()) {
      datacenterReport = optionalDatacenterReport.get();
    } else {
      datacenterReport = new DataCenterConnectionReport(id, this);
      this.addDataCenter(datacenterReport);
    }
    NodeConnectionReport nodeReport =
        new NodeConnectionReport(node.getId(), addressList, serverAddress, datacenterReport);
    datacenterReport.addNode(nodeReport);
  }

  private void addDataCenter(DataCenterConnectionReport dataCenter) {
    assert dataCenter.getParent().orElse(null) == this;
    this.dataCenters.add(dataCenter);
  }

  public Collection<DataCenterConnectionReport> getDataCenters() {
    return dataCenters;
  }

  @Override
  @JsonIgnore
  public Long getActiveConnections() {
    return getDataCenters()
        .stream()
        .mapToLong(DataCenterConnectionReport::getActiveConnections)
        .sum();
  }

  @Override
  public Cluster getCluster() {
    return null;
  }

  @JsonIgnore
  @Override
  public Optional<NodeProperties> getParent() {
    return Optional.empty();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    ClusterConnectionReport that = (ClusterConnectionReport) o;

    return this.getId().equals(that.getId());
  }

  @Override
  public int hashCode() {
    return dataCenters != null ? dataCenters.hashCode() : 0;
  }
}
