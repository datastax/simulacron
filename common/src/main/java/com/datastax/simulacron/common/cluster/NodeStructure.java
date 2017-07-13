package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Defines some useful convenience methods for an object that exists at the 'Node' level of Cluster
 * (1 -> *) DataCenter (1 -> *) Node.
 *
 * @param <C> Cluster type
 * @param <D> DataCenter type
 */
public interface NodeStructure<C extends ClusterStructure, D extends DataCenterStructure<C, ?>>
    extends Identifiable {

  /** @return the data center this node belongs to. */
  @JsonIgnore
  D getDataCenter();

  /** @return the cluster this node belongs to. */
  @JsonIgnore
  default C getCluster() {
    if (getDataCenter() != null) {
      return getDataCenter().getCluster();
    }
    return null;
  }
}
