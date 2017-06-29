package com.datastax.simulacron.common.cluster;

import java.util.Map;

public abstract class ClusterBuilderBase<S extends ClusterBuilderBase<S, P>, P extends Cluster>
    extends NodePropertiesBuilder<S, P> {

  int[] nodes = null;

  private S myself;

  @SuppressWarnings("unchecked")
  public ClusterBuilderBase(Class<?> selfType) {
    super(selfType);
    this.myself = (S) selfType.cast(this);
  }

  /**
   * Convenience method to preprovision data centers and nodes with default settings, each element
   * of nodeCount is a datacenter with the number of nodes as its value.
   *
   * @param nodeCount Array with each element respresenting a DataCenter with its node counts.
   */
  public S withNodes(int... nodeCount) {
    this.nodes = nodeCount;
    return myself;
  }

  public abstract P baseCluster(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo);

  /** @return Constructs a {@link Cluster} from this builder. Can be called multiple times. */
  public P build() {
    P cluster = baseCluster(name, id, cassandraVersion, dseVersion, peerInfo);
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
