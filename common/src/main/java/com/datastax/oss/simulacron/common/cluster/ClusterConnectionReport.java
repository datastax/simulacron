/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * Represent a class that contains the connections of a particular cluster. It's useful for encoding
 * the results with JSON.
 */
@JsonIgnoreProperties(value = {"name"})
public class ClusterConnectionReport extends ConnectionReport
    implements ClusterStructure<DataCenterConnectionReport, NodeConnectionReport> {

  @JsonManagedReference
  @JsonProperty("data_centers")
  private final Collection<DataCenterConnectionReport> dataCenters = new TreeSet<>();

  @SuppressWarnings("unused")
  ClusterConnectionReport() {
    // Default constructor for jackson deserialization.
    this(null);
  }

  public ClusterConnectionReport(Long id) {
    super(id);
  }

  @Override
  public ClusterConnectionReport getRootReport() {
    return this;
  }

  /**
   * Convenience method for adding a single node's connection to ClusterConnectionReport
   *
   * @param node Node to add
   * @param addressList client side of the connections this node has
   * @param serverAddress the address where this node is listening
   * @return report for added node.
   */
  public NodeConnectionReport addNode(
      AbstractNode node, List<SocketAddress> addressList, SocketAddress serverAddress) {
    Long dcId = node.getDataCenter().getId();
    Optional<DataCenterConnectionReport> optionalDatacenterReport =
        dataCenters.stream().filter(dc -> dc.getId().equals(dcId)).findFirst();
    DataCenterConnectionReport datacenterReport;
    if (optionalDatacenterReport.isPresent()) {
      datacenterReport = optionalDatacenterReport.get();
    } else {
      datacenterReport = new DataCenterConnectionReport(dcId, this);
      this.addDataCenter(datacenterReport);
    }
    NodeConnectionReport nodeReport =
        new NodeConnectionReport(node.getId(), addressList, serverAddress, datacenterReport);
    datacenterReport.addNode(nodeReport);
    return nodeReport;
  }

  void addDataCenter(DataCenterConnectionReport dataCenter) {
    assert dataCenter.getCluster() == this;
    this.dataCenters.add(dataCenter);
  }

  public Collection<DataCenterConnectionReport> getDataCenters() {
    return dataCenters;
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

  @Override
  public List<SocketAddress> getConnections() {
    return getNodes().stream()
        .flatMap(n -> n.getConnections().stream())
        .collect(Collectors.toList());
  }
}
