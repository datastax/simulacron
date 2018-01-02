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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Represents a cluster, which contains data centers, which contains nodes. */
public class ClusterSpec extends AbstractCluster<DataCenterSpec, NodeSpec> {

  @JsonIgnore private final transient AtomicLong dcCounter = new AtomicLong(0);

  @JsonIgnore private int numberOfTokens;

  ClusterSpec() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap(), 1);
  }

  public ClusterSpec(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      int numberOfTokens) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
    this.numberOfTokens = numberOfTokens;
  }

  /** @return the number of tokens to be assigned to each node */
  public int getNumberOfTokens() {
    return this.numberOfTokens;
  }

  /**
   * Constructs a builder for a {@link DataCenterSpec} that will be added to this cluster. On
   * construction the created {@link DataCenterSpec} will be added to this cluster.
   *
   * @return a Builder to create a {@link DataCenterSpec} in this cluster.
   */
  public DataCenterSpec.Builder addDataCenter() {
    return new DataCenterSpec.Builder(this, dcCounter.getAndIncrement());
  }

  /** @return A builder for making a ClusterSpec. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends NodePropertiesBuilder<Builder, ClusterSpec> {

    int[] nodes = null;
    private int numberOfTokens = 1;

    @SuppressWarnings("unchecked")
    public Builder() {
      super(Builder.class);
    }

    /**
     * Convenience method to preprovision data centers and nodes with default settings, each element
     * of nodeCount is a datacenter with the number of nodes as its value.
     *
     * @param nodeCount Array with each element representing a data center with its node counts.
     * @return builder with dcs configured.
     */
    public Builder withNodes(int... nodeCount) {
      this.nodes = nodeCount;
      return this;
    }

    /**
     * Convenience method to set the number of virtual nodes (tokens) assigned for each node of each
     * datacenter. If not used the value 1 will be assigned and the tokens will be divided according
     * to cluster size.
     *
     * @param numberOfTokens Integer with number of tokens.
     * @return builder with dcs configured.
     */
    public Builder withNumberOfTokens(int numberOfTokens) {
      this.numberOfTokens = numberOfTokens;
      return this;
    }

    /** @return Constructs a {@link ClusterSpec} from this builder. Can be called multiple times. */
    public ClusterSpec build() {
      ClusterSpec cluster =
          new ClusterSpec(name, id, cassandraVersion, dseVersion, peerInfo, numberOfTokens);
      if (nodes != null) {
        for (int i = 1; i <= nodes.length; i++) {
          int nodeCount = nodes[i - 1];
          DataCenterSpec dc = cluster.addDataCenter().withName("dc" + i).build();
          for (int j = 1; j <= nodeCount; j++) {
            dc.addNode().withName("node" + j).build();
          }
        }
      }
      return cluster;
    }
  }
}
