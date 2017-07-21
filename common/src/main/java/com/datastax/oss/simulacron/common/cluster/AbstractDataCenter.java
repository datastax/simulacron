/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

public abstract class AbstractDataCenter<C extends AbstractCluster, N extends AbstractNode>
    extends AbstractNodeProperties implements DataCenterStructure<C, N> {

  // json managed reference is used to indicate a two way linking between the 'parent' (datacenter) and 'children'
  // (nodes) in a json tree.  This tells the jackson mapping to tie child nodes to this dc on deserialization.
  @JsonManagedReference private final Collection<N> nodes = new ConcurrentSkipListSet<>();

  // back reference is used to indicate the parent of this node while deserializing should be tied to this field.
  @JsonBackReference private final C parent;

  @SuppressWarnings("unchecked")
  public AbstractDataCenter(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      C parent) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
    this.parent = parent;
    if (this.parent != null) {
      parent.addDataCenter(this);
    }
  }

  @Override
  public C getCluster() {
    return parent;
  }

  @Override
  public Collection<N> getNodes() {
    return nodes;
  }

  /**
   * Intended to be called in {@link AbstractNode} construction to add the {@link AbstractNode} to
   * this data center.
   *
   * @param node The node to tie to this data center.
   */
  @SuppressWarnings("unchecked")
  <K extends AbstractNode> void addNode(K node) {
    // K type is needed as self reference is not possible in AbstractNode
    assert node.getDataCenter() == this;
    this.nodes.add((N) node);
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
}
