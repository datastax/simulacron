package com.datastax.simulacron.common.cluster;

import java.util.Map;

/**
 * A convenience wrapper over {@link Cluster} to rename it so it doesn't clash with a driver's
 * Cluster type.
 */
public class SimulacronCluster extends Cluster {

  SimulacronCluster(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
  }

  public static class Builder extends ClusterBuilderBase<Builder, SimulacronCluster> {

    Builder() {
      super(SimulacronCluster.Builder.class);
    }

    @Override
    public SimulacronCluster baseCluster(
        String name,
        Long id,
        String cassandraVersion,
        String dseVersion,
        Map<String, Object> peerInfo) {
      return new SimulacronCluster(name, id, cassandraVersion, dseVersion, peerInfo);
    }
  }

  /** Overrides {@link Cluster#builder()} to throw an {@link UnsupportedOperationException}. */
  public static Cluster.Builder builder() {
    throw new UnsupportedOperationException(
        "SimulacronCluster.builder() is unsupported, use cbuilder() instead");
  }

  /** @return a base builder. */
  public static SimulacronCluster.Builder cbuilder() {
    return new SimulacronCluster.Builder();
  }
}
