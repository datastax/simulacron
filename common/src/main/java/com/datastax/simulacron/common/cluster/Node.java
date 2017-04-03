package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public class Node extends AbstractNodeProperties {

  @JsonProperty private final SocketAddress address;

  @JsonBackReference private final DataCenter parent;

  Node() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap(), null);
  }

  public Node(
      SocketAddress address,
      String name,
      UUID id,
      String cassandraVersion,
      Map<String, Object> peerInfo,
      DataCenter parent) {
    super(name, id, cassandraVersion, peerInfo);
    this.address = address;
    this.parent = parent;
    if (this.parent != null) {
      parent.addNode(this);
    }
  }

  public SocketAddress getAddress() {
    return address;
  }

  /**
   * @return The {@link DataCenter} this node belongs to, otherwise null if it does not have one.
   */
  public DataCenter getDataCenter() {
    return parent;
  }

  /**
   * @return The {@link Cluster} associated this node belongs to, otherwise null if it does not
   *     belong to one.
   */
  public Cluster getCluster() {
    return Optional.ofNullable(parent).map(DataCenter::getCluster).orElse(null);
  }

  @Override
  public String toString() {
    return toStringWith(", address=" + address);
  }

  // programmatic builder.

  public static Builder builder() {
    return new Builder(null);
  }

  public static Builder builder(DataCenter dataCenter) {
    return new Builder(dataCenter);
  }

  @Override
  @JsonIgnore
  public Optional<NodeProperties> getParent() {
    return Optional.ofNullable(parent);
  }

  public static class Builder extends NodePropertiesBuilder<Builder, DataCenter> {

    private SocketAddress address = null;

    Builder(DataCenter parent) {
      super(Builder.class, parent);
    }

    public Builder withAddress(SocketAddress address) {
      this.address = address;
      return this;
    }

    public Node build() {
      return new Node(address, name, id, cassandraVersion, peerInfo, parent);
    }
  }
}
