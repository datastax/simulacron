package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.SimulacronCluster;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;

/** A convenience server builder for {@link SimulacronCluster}. */
public class SimulacronServer {

  /** see {@link Server#builder()} */
  public static Server.Builder<SimulacronCluster> builder() {
    return builder(Server.createEventLoop(), Server.channelClass());
  }

  /** see {@link Server#builder(EventLoopGroup, Class)} */
  public static Server.Builder<SimulacronCluster> builder(
      EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> channelClass) {
    return Server.builder(eventLoopGroup, channelClass, SimulacronCluster::cbuilder);
  }
}
