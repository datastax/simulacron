package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.stream.Collectors;

public class DataCenter extends AbstractNodeProperties {

  private final List<Node> nodes = new ArrayList<>();

  @JsonCreator
  DataCenter(
      @JsonProperty("name") String name,
      @JsonProperty("id") UUID id,
      @JsonProperty("cassandra_version") String cassandraVersion,
      @JsonProperty("peer_info") Map<String, Object> peerInfo) {
    this(name, id, cassandraVersion, peerInfo, null);
  }

  DataCenter(
      String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo, Cluster parent) {
    super(name, id, cassandraVersion, peerInfo, parent);
    if (parent != null) {
      parent.addDataCenter(this);
    }
  }

  public List<Node> getNodes() {
    return Collections.unmodifiableList(nodes);
  }

  void addNode(Node node) {
    assert node.getParent().orElse(null) == this;
    this.nodes.add(node);
  }

  public Node.Builder addNode() {
    return new Node.Builder(this);
  }

  public static Builder builder(Cluster cluster) {
    return new Builder(cluster);
  }

  @Override
  public String toString() {
    return toStringWith(
        ", nodes="
            + nodes.stream().map(n -> n.getId().toString()).collect(Collectors.joining(",")));
  }

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    Builder(Cluster parent) {
      super(Builder.class, parent);
    }

    public DataCenter build() {
      return new DataCenter(name, id, cassandraVersion, peerInfo, parent);
    }
  }
}
