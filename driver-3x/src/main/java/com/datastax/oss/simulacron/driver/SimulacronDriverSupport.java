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
package com.datastax.oss.simulacron.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.NettyOptions;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import io.netty.channel.EventLoopGroup;

import static java.util.concurrent.TimeUnit.SECONDS;

/** Convenience class for creating ClusterSpec and NodeSpec builders with ideal defaults. */
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
   * @param node to connect to as contact point.
   * @return a builder to connect to the input node.
   */
  public static Cluster.Builder defaultBuilder(BoundNode node) {
    return defaultBuilder().withPort(node.port()).addContactPoints(node.inet());
  }

  /**
   * @param cluster cluster to connect to.
   * @return a builder to connect to the input cluster.
   */
  public static Cluster.Builder defaultBuilder(BoundCluster cluster) {
    BoundNode node = cluster.node(0);
    return defaultBuilder(node);
  }

  /** @return A default cluster builder using {@link #nonQuietClusterCloseOptions}. */
  public static Cluster.Builder defaultBuilder() {
    return Cluster.builder().withNettyOptions(nonQuietClusterCloseOptions);
  }
}
