package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Represents a cluster, which contains {@link DataCenter}s which contains {@link Node}s. */
public class Cluster extends AbstractCluster<DataCenter, Node> {

  @JsonIgnore private final transient AtomicLong dcCounter = new AtomicLong(0);

  Cluster() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap());
  }

  public Cluster(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
  }

  /**
   * Constructs a builder for a {@link DataCenter} that will be added to this cluster. On
   * construction the created {@link DataCenter} will be added to this cluster.
   *
   * @return a Builder to create a {@link DataCenter} in this cluster.
   */
  public DataCenter.Builder addDataCenter() {
    return new DataCenter.Builder(this, dcCounter.getAndIncrement());
  }

  /** @return A builder for making a Cluster. */
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    int[] nodes = null;

    @SuppressWarnings("unchecked")
    public Builder() {
      super(Builder.class);
    }

    /**
     * Convenience method to preprovision data centers and nodes with default settings, each element
     * of nodeCount is a datacenter with the number of nodes as its value.
     *
     * @param nodeCount Array with each element respresenting a DataCenter with its node counts.
     * @return builder with dcs configured.
     */
    public Builder withNodes(int... nodeCount) {
      this.nodes = nodeCount;
      return this;
    }

    /** @return Constructs a {@link Cluster} from this builder. Can be called multiple times. */
    public Cluster build() {
      Cluster cluster = new Cluster(name, id, cassandraVersion, dseVersion, peerInfo);
      if (nodes != null) {
        for (int i = 1; i <= nodes.length; i++) {
          int nodeCount = nodes[i - 1];
          DataCenter dc = cluster.addDataCenter().withName("dc" + i).build();
          for (int j = 1; j <= nodeCount; j++) {
            dc.addNode().withName("node" + j).build();
          }
        }
      }
      return cluster;
    }
  }
}
