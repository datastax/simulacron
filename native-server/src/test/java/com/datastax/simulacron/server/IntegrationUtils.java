package com.datastax.simulacron.server;

import com.datastax.driver.core.NettyOptions;
import io.netty.channel.EventLoopGroup;

import static java.util.concurrent.TimeUnit.SECONDS;

public class IntegrationUtils {

  /**
   * A custom {@link NettyOptions} that shuts down the {@link EventLoopGroup} after no quiet time.
   */
  static NettyOptions nonQuietClusterCloseOptions =
      new NettyOptions() {
        @Override
        public void onClusterClose(EventLoopGroup eventLoopGroup) {
          eventLoopGroup.shutdownGracefully(0, 15, SECONDS).syncUninterruptibly();
        }
      };

  /** @return A default cluster builder using {@link #nonQuietClusterCloseOptions}. */
  static com.datastax.driver.core.Cluster.Builder defaultBuilder() {
    return com.datastax.driver.core.Cluster.builder().withNettyOptions(nonQuietClusterCloseOptions);
  }
}
