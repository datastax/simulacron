package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.SocketAddress;
import java.util.Map;
import java.util.UUID;

public class Node extends AbstractNodeProperties {

  private SocketAddress address;

  @JsonCreator
  Node(
      @JsonProperty("address") SocketAddress address,
      @JsonProperty("name") String name,
      @JsonProperty("id") UUID id,
      @JsonProperty("cassandra_version") String cassandraVersion,
      @JsonProperty("peer_info") Map<String, Object> peerInfo) {
    this(address, name, id, cassandraVersion, peerInfo, null);
  }

  Node(
      SocketAddress address,
      String name,
      UUID id,
      String cassandraVersion,
      Map<String, Object> peerInfo,
      DataCenter parent) {
    super(name, id, cassandraVersion, peerInfo, parent);
    this.address = address;
    if (parent != null) {
      parent.addNode(this);
    }
  }

  public SocketAddress getAddress() {
    return address;
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
