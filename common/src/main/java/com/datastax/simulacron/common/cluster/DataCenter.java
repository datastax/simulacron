package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Represents a DataCenter which is a member of a {@link Cluster} that has {@link Node}s belonging
 * to it.
 */
public class DataCenter extends AbstractNodeProperties {

  // json managed reference is used to indicate a two way linking between the 'parent' (datacenter) and 'children'
  // (nodes) in a json tree.  This tells the jackson mapping to tie child nodes to this dc on deserialization.
  @JsonManagedReference private final Collection<Node> nodes = new ConcurrentLinkedQueue<>();

  // back reference is used to indicate the parent of this node while deserializing should be tied to this field.
  @JsonBackReference private final Cluster parent;

  // A counter to assign unique ids to nodes belonging to this dc.
  @JsonIgnore private final transient AtomicLong nodeCounter = new AtomicLong(0);

  DataCenter() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap(), null);
  }

  DataCenter(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      Cluster parent) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
    this.parent = parent;
    if (this.parent != null) {
      parent.addDataCenter(this);
    }
  }

  /** @return The {@link Cluster} associated this belongs to otherwise null. */
  @JsonIgnore
  public Cluster getCluster() {
    return parent;
  }

  /** @return The nodes belonging to this data center. */
  public Collection<Node> getNodes() {
    return nodes;
  }

  /**
   * Intended to be called in {@link Node} construction to add the {@link Node} to this data center.
   *
   * @param node The node to tie to this data center.
   */
  void addNode(Node node) {
    assert node.getParent().orElse(null) == this;
    this.nodes.add(node);
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

  @Override
  public String toString() {
    return toStringWith(
        ", nodes="
            + nodes.stream().map(n -> n.getId().toString()).collect(Collectors.joining(",")));
  }

  @Override
  public Optional<NodeProperties> getParent() {
    return Optional.ofNullable(parent);
  }

  @Override
  public Long getActiveConnections() {
    return nodes.stream().mapToLong(NodeProperties::getActiveConnections).sum();
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
