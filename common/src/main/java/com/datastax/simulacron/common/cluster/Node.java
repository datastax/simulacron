package com.datastax.simulacron.common.cluster;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;

/**
 * Represents a {@link Node} which may belong to a {@link Cluster} (via a {@link DataCenter}
 * relationship) or it may be standalone to represent a 'single node' cluster.
 *
 * <p>A {@link Node} has an address which indicates what ip address and port the node is listening
 * on.
 */
public class Node extends AbstractNode<Cluster, DataCenter> {

  Node() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, null, Collections.emptyMap(), null);
  }

  public Node(
      SocketAddress address,
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      DataCenter parent) {
    super(address, name, id, cassandraVersion, dseVersion, peerInfo, parent);
  }

  /**
   * Constructs a {@link Builder} for defining a {@link Node} that has no parent {@link Cluster}.
   *
   * @return Builder for creating {@link Node}
   */
  public static Builder builder() {
    return new Builder(null, null);
  }

  public static class Builder extends NodePropertiesBuilder<Builder, DataCenter> {

    private SocketAddress address = null;

    Builder(DataCenter parent, Long id) {
      super(Builder.class, parent);
      this.id = id;
    }

    /**
     * Sets the address that the Node should be listening on.
     *
     * @param address address to listen on
     * @return this builder
     */
    public Builder withAddress(SocketAddress address) {
      this.address = address;
      return this;
    }

    /** @return Constructs a {@link Node} from this builder. Can be called multiple times. */
    public Node build() {
      return new Node(address, name, id, cassandraVersion, dseVersion, peerInfo, parent);
    }
  }
}
