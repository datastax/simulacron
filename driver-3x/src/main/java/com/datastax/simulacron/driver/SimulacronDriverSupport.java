package com.datastax.simulacron.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.SimulacronCluster;
import io.netty.channel.EventLoopGroup;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Convenience class for creating Cluster and Node builders with ideal defaults. */
public class SimulacronDriverSupport {

  /**
   * A custom {@link NettyOptions} that shuts down the {@link EventLoopGroup} after no quiet time.
   */
  public static NettyOptions nonQuietClusterCloseOptions =
      new NettyOptions() {
        @Override
        public void onClusterClose(EventLoopGroup eventLoopGroup) {
          eventLoopGroup.shutdownGracefully(0, 15, SECONDS).syncUninterruptibly();
        }
      };

  /**
   * @return A default cluster builder that has contact point and port preconfigured to the input
   *     simulacron node.
   * @param node to connect to as contact point.
   * @return a builder to connect to the input node.
   */
  public static Cluster.Builder defaultBuilder(Node node) {
    return defaultBuilder().withPort(node.port()).addContactPoints(node.inet());
  }

  /**
   * @return A default cluster builder that has contact points and ports preconfigured to the input
   *     simulacron cluster.
   * @param cluster cluster to connect to.
   * @return a builder to connect to the input cluster.
   */
  public static Cluster.Builder defaultBuilder(
      com.datastax.simulacron.common.cluster.Cluster cluster) {
    Node node = cluster.node(0);
    return defaultBuilder(node);
  }

  /** @return A default cluster builder using {@link #nonQuietClusterCloseOptions}. */
  public static Cluster.Builder defaultBuilder() {
    return Cluster.builder().withNettyOptions(nonQuietClusterCloseOptions);
  }

  /** @return A default node builder */
  public static Node.Builder node() {
    return Node.builder();
  }

  /** @return a default {@link SimulacronCluster} builder * */
  public static SimulacronCluster.Builder cluster() {
    return SimulacronCluster.cbuilder();
  }
}
