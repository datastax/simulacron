package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.*;
import java.util.stream.Collectors;

public class DataCenter extends AbstractNodeProperties {

  @JsonManagedReference private final List<Node> nodes = new ArrayList<>();

  @JsonBackReference private final Cluster parent;

  DataCenter() {
    // Default constructor for jackson deserialization.
    this(null, null, null, Collections.emptyMap(), null);
  }

  DataCenter(
      String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo, Cluster parent) {
    super(name, id, cassandraVersion, peerInfo);
    this.parent = parent;
    if (this.parent != null) {
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

  @Override
  public Optional<NodeProperties> getParent() {
    return Optional.of(parent);
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
