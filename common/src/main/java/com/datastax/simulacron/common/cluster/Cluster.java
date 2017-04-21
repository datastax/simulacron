package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonManagedReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Represents a cluster, which contains {@link DataCenter}s which contains {@link Node}s. */
public class Cluster extends AbstractNodeProperties {

  // json managed reference is used to indicate a two way linking between the 'parent' (cluster) and 'children'
  // (datacenters) in a json tree.  This tells the jackson mapping to tie child DCs to this cluster on deserialization.
  @JsonManagedReference
  @JsonProperty("data_centers")
  private final List<DataCenter> dataCenters = new ArrayList<>();

  @JsonIgnore private final transient AtomicLong dcCounter = new AtomicLong(0);

  Cluster() {
    // Default constructor for jackson deserialization.
    this(null, null, null, null, Collections.emptyMap());
  }

  Cluster(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
  }

  /** @return The {@link DataCenter}s belonging to this cluster. */
  public Collection<DataCenter> getDataCenters() {
    return dataCenters;
  }

  /**
   * @return All nodes belonging to {@link DataCenter}s in this cluster. Note that this method
   *     builds a new collection on every invocation, so it is recommended to not call this
   *     repeatedly without a good reason.
   */
  @JsonIgnore
  public List<Node> getNodes() {
    return dataCenters.stream().flatMap(dc -> dc.getNodes().stream()).collect(Collectors.toList());
  }

  /**
   * Intended to be called in {@link DataCenter} construction to add the {@link DataCenter} to this
   * cluster.
   *
   * @param dataCenter The data center to tie to this cluster.
   */
  void addDataCenter(DataCenter dataCenter) {
    assert dataCenter.getParent().orElse(null) == this;
    this.dataCenters.add(dataCenter);
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

  @Override
  public String toString() {
    return toStringWith(
        ", dataCenters="
            + dataCenters.stream().map(d -> d.getId().toString()).collect(Collectors.joining(",")));
  }

  @Override
  @JsonIgnore
  public Optional<NodeProperties> getParent() {
    return Optional.empty();
  }

  public static class Builder extends NodePropertiesBuilder<Builder, Cluster> {

    int[] nodes = null;

    Builder() {
      super(Builder.class);
    }

    /**
     * Convenience method to preprovision data centers and nodes with default settings, each element
     * of nodeCount is a datacenter with the number of nodes as its value.
     *
     * @param nodeCount Array with each element respresenting a DataCenter with its node counts.
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
