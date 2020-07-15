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
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Represents a data center which is a member of a cluster that has nodes belonging to it. */
public class DataCenterSpec extends AbstractDataCenter<ClusterSpec, NodeSpec> {

  // A counter to assign unique ids to nodes belonging to this dc.
  @JsonIgnore private final transient AtomicLong nodeCounter = new AtomicLong(0);

  DataCenterSpec() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap(), null);
  }

  public DataCenterSpec(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      ClusterSpec parent) {
    super(name, id, cassandraVersion, dseVersion, peerInfo, parent);
  }

  /**
   * Constructs a builder for a {@link NodeSpec} that will be added to this data center. On
   * construction the created {@link NodeSpec} will be added to this data center.
   *
   * @return a Builder to create a {@link NodeSpec} in this data center.
   */
  public NodeSpec.Builder addNode() {
    return new NodeSpec.Builder(this, nodeCounter.getAndIncrement(), UUID.randomUUID());
  }

  public static class Builder extends NodePropertiesBuilder<Builder, ClusterSpec> {

    Builder(ClusterSpec parent, Long id) {
      super(Builder.class, parent);
      this.id = id;
    }

    /**
     * @return Constructs a {@link DataCenterSpec} from this builder. Can be called multiple times.
     */
    public DataCenterSpec build() {
      return new DataCenterSpec(name, id, cassandraVersion, dseVersion, peerInfo, parent);
    }
  }
}
