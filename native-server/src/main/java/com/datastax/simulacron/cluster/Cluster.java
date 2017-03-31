package com.datastax.simulacron.cluster;

import com.datastax.simulacron.server.Server;

import java.util.*;
import java.util.stream.Collectors;

public class Cluster extends AbstractNodeProperties {

  private final List<DataCenter> dataCenters = new ArrayList<>();

  Cluster(String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, peerInfo, null);
  }

  public List<DataCenter> dataCenters() {
    return Collections.unmodifiableList(dataCenters);
  }

  void addDataCenter(DataCenter dataCenter) {
    assert dataCenter.parent().orElse(null) == this;
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
            + dataCenters.stream().map(d -> d.id().toString()).collect(Collectors.joining(",")));
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
      if (id == null) {
        id = UUID.randomUUID();
      }
      if (name == null) {
        name = id.toString();
      }
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

  public static void main(String args[]) {
    Cluster cluster = Cluster.builder().withName("cluster1").withNodes(10, 10).build();

    Server server = new Server();
    server.bind(cluster);
  }
}
