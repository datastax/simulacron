package com.datastax.simulacron.cluster;

import java.util.*;
import java.util.stream.Collectors;

public class DataCenter extends AbstractNodeProperties {

  private List<Node> nodes = new ArrayList<>();

  DataCenter(
      String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo, Cluster parent) {
    super(name, id, cassandraVersion, peerInfo, parent);
    parent.addDataCenter(this);
  }

  public List<Node> nodes() {
    return Collections.unmodifiableList(nodes);
  }

  void addNode(Node node) {
    assert node.parent().orElse(null) == this;
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
        ", nodes=" + nodes.stream().map(n -> n.id().toString()).collect(Collectors.joining(",")));
  }

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    Builder(Cluster parent) {
      super(Builder.class, parent);
    }

    public DataCenter build() {
      if (id == null) {
        id = UUID.randomUUID();
      }
      if (name == null) {
        name = id.toString();
      }
      return new DataCenter(name, id, cassandraVersion, peerInfo, parent);
    }
  }
}
