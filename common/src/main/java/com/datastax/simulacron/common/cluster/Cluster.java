package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

public class Cluster extends AbstractNodeProperties {

  @JsonManagedReference
  @JsonProperty("data_centers")
  private final List<DataCenter> dataCenters = new ArrayList<>();

  Cluster() {
    // Default constructor for jackson deserialization.
    this(null, null, null, Collections.emptyMap());
  }

  Cluster(String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, peerInfo);
  }

  public List<DataCenter> getDataCenters() {
    return Collections.unmodifiableList(dataCenters);
  }

  void addDataCenter(DataCenter dataCenter) {
    assert dataCenter.getParent().orElse(null) == this;
    this.dataCenters.add(dataCenter);
  }

  public DataCenter.Builder addDataCenter() {
    return new DataCenter.Builder(this);
  }

  public static Builder builder() {
    return new Builder();
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

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    int[] nodes = null;

    Builder() {
      super(Builder.class);
    }

    /**
     * Convenience method to preprovision data centers and nodes with default settings, each element
     * of nodeCount is a datacenter with the number of nodes as its value.
     *
     * @param nodeCount Array with each element respresenting a DataCenter with its node counts.
     */
    public Builder withNodes(int... nodeCount) {
      this.nodes = nodeCount;
      return this;
    }

    public Cluster build() {
      Cluster cluster = new Cluster(name, id, cassandraVersion, peerInfo);
      if (nodes != null) {
        for (int i = 1; i <= nodes.length; i++) {
          int nodeCount = nodes[i - 1];
          DataCenter dc = cluster.addDataCenter().withName("dc" + i).build();
          for (int j = 1; j <= nodeCount; j++) {
            dc.addNode().withName("node" + j).build();
          }
        }
      }
      return cluster;
    }
  }
}
