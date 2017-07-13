package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Represents a cluster, which contains data centers, which contains nodes. */
public class ClusterSpec extends AbstractCluster<DataCenterSpec, NodeSpec> {

  @JsonIgnore private final transient AtomicLong dcCounter = new AtomicLong(0);

  ClusterSpec() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap());
  }

  public ClusterSpec(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
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

    /** @return Constructs a {@link ClusterSpec} from this builder. Can be called multiple times. */
    public ClusterSpec build() {
      ClusterSpec cluster = new ClusterSpec(name, id, cassandraVersion, dseVersion, peerInfo);
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
