package com.datastax.simulacron.test;

import com.datastax.driver.core.NettyOptions;
import com.datastax.simulacron.common.cluster.Cluster;
import io.netty.channel.EventLoopGroup;

import static java.util.concurrent.TimeUnit.SECONDS;

public class IntegrationUtils {

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

  /** @return A default cluster builder using {@link #nonQuietClusterCloseOptions}. */
  public static com.datastax.driver.core.Cluster.Builder defaultBuilder(Cluster cluster) {
    return defaultBuilder().addContactPointsWithPorts(cluster.node(0, 0).inet());
  }

  public static com.datastax.driver.core.Cluster.Builder defaultBuilder() {
    return com.datastax.driver.core.Cluster.builder().withNettyOptions(nonQuietClusterCloseOptions);
  }
}
