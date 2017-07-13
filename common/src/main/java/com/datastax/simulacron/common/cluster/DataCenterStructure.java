package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;

/**
 * Defines some useful convenience methods for an object that exists at the 'DataCenter' level of
 * Cluster (1 -> *) DataCenter (1 -> *) Node.
 *
 * @param <C> Cluster type
 * @param <N> Node type.
 */
public interface DataCenterStructure<C extends ClusterStructure, N extends NodeStructure>
    extends Identifiable {

  /** @return the nodes belonging to this data center. */
  Collection<N> getNodes();

  /**
   * Convenience method to look up node by id.
   *
   * @param id The id of the node.
   * @return The node if found or null.
   */
  default N node(long id) {
    return getNodes().stream().filter(n -> n.getId() == id).findAny().orElse(null);
  }

  /** @return the cluster this data center belongs to. */
  @JsonIgnore
  C getCluster();
}
