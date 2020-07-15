/*
 * Copyright DataStax, Inc.
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
