package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Represents a DataCenter which is a member of a {@link Cluster} that has {@link Node}s belonging
 * to it.
 */
public class DataCenter extends AbstractDataCenter<Cluster, Node> {

  // A counter to assign unique ids to nodes belonging to this dc.
  @JsonIgnore private final transient AtomicLong nodeCounter = new AtomicLong(0);

  DataCenter() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap(), null);
  }

  public DataCenter(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      Cluster parent) {
    super(name, id, cassandraVersion, dseVersion, peerInfo, parent);
  }

  /**
   * Constructs a builder for a {@link Node} that will be added to this data center. On construction
   * the created {@link Node} will be added to this data center.
   *
   * @return a Builder to create a {@link Node} in this data center.
   */
  public Node.Builder addNode() {
    return new Node.Builder(this, nodeCounter.getAndIncrement());
  }

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    Builder(Cluster parent, Long id) {
      super(Builder.class, parent);
      this.id = id;
    }

    /** @return Constructs a {@link DataCenter} from this builder. Can be called multiple times. */
    public DataCenter build() {
      return new DataCenter(name, id, cassandraVersion, dseVersion, peerInfo, parent);
    }
  }
}
