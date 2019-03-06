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

import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

/**
 * Represents a {@link NodeSpec} which may belong to a cluster (via a data center relationship) or
 * it may be standalone to represent a 'single node' cluster.
 *
 * <p>A {@link NodeSpec} has an address which indicates what ip address and port the node is
 * listening on.
 */
public class NodeSpec extends AbstractNode<ClusterSpec, DataCenterSpec> {

  NodeSpec() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, null, null, Collections.emptyMap(), null);
  }

  public NodeSpec(
      SocketAddress address,
      String name,
      Long id,
      UUID hostId,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      DataCenterSpec parent) {
    super(address, name, id, hostId, cassandraVersion, dseVersion, peerInfo, parent);
  }

  /**
   * Constructs a {@link Builder} for defining a {@link NodeSpec} that has no parent {@link
   * ClusterSpec}.
   *
   * @return Builder for creating {@link NodeSpec}
   */
  public static Builder builder() {
    return new Builder(null, null, null);
  }

  public static class Builder extends NodePropertiesBuilder<Builder, DataCenterSpec> {

    private SocketAddress address = null;

    Builder(DataCenterSpec parent, Long id, UUID hostId) {
      super(Builder.class, parent);
      this.id = id;
      this.hostId = hostId;
    }

    /**
     * Sets the address that the NodeSpec should be listening on.
     *
     * @param address address to listen on
     * @return this builder
     */
    public Builder withAddress(SocketAddress address) {
      this.address = address;
      return this;
    }

    /** @return Constructs a {@link NodeSpec} from this builder. Can be called multiple times. */
    public NodeSpec build() {
      return new NodeSpec(
          address, name, id, hostId, cassandraVersion, dseVersion, peerInfo, parent);
    }
  }
}
