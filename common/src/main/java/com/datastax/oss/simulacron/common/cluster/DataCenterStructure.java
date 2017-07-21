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
