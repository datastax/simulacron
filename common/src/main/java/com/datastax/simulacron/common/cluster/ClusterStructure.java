package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * /** Defines some useful convenience methods for an object that exists at the 'Cluster' level of
 * Cluster (1 -> *) DataCenter (1 -> *) Node.
 *
 * @param <D> DataCenter type
 * @param <N> Node type
 */
public interface ClusterStructure<D extends DataCenterStructure<?, N>, N extends NodeStructure>
    extends Identifiable {

  /** @return The {@link DataCenter}s belonging to this cluster. */
  Collection<D> getDataCenters();

  /**
   * @return All nodes belonging to {@link DataCenter}s in this cluster. Note that this method
   *     builds a new collection on every invocation, so it is recommended to not call this
   *     repeatedly without a good reason.
   */
  @JsonIgnore
  default Collection<N> getNodes() {
    return getDataCenters()
        .stream()
        .flatMap(dc -> dc.getNodes().stream())
        .collect(Collectors.toList());
  }

  /**
   * Convenience method to find the DataCenter with the given id.
   *
   * @param id id of the data center.
   * @return the data center if found or null.
   */
  default D dc(long id) {
    return getDataCenters().stream().filter(d -> d.getId() == id).findAny().orElse(null);
  }

  /**
   * Convenience method to find the Node in DataCenter 0 with the given id. This is a shortcut for
   * <code>node(0, X)</code> as it is common for clusters to only have 1 dc.
   *
   * @param id id of the node.
   * @return the node if found in dc 0 or null
   */
  default N node(long id) {
    return node(0, id);
  }

  /**
   * Convenience method to find the Node with the given DataCenter id and node id.
   *
   * @param dcId id of the data center.
   * @param nodeId id of the node.
   * @return the node if found or null
   */
  default N node(long dcId, long nodeId) {
    return Optional.ofNullable(dc(dcId)).map(d -> d.node(nodeId)).orElse(null);
  }
}
