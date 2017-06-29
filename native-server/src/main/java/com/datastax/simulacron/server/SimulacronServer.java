package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.SimulacronCluster;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A convenience server builder for {@link SimulacronCluster}. */
public class SimulacronServer {

  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  /**
   * Convenience method that provides a {@link Server.Builder} that uses NIO for networking traffic,
   * which should be the typical case.
   *
   * @return Builder that is set up with {@link NioEventLoopGroup} and {@link
   *     NioServerSocketChannel}.
   */
  public static Server.Builder<SimulacronCluster> builder() {
    if (Epoll.isAvailable()) {
      logger.debug("Detected epoll support, using EpollEventLoopGroup");
      return builder(new EpollEventLoopGroup(), EpollServerSocketChannel.class);
    } else {
      logger.debug("Could not locate native transport (epoll), using NioEventLoopGroup");
      return builder(new NioEventLoopGroup(), NioServerSocketChannel.class);
    }
  }

  /**
   * Constructs a {@link Server.Builder} that uses the given {@link EventLoopGroup} and {@link
   * ServerChannel} to construct a {@link ServerBootstrap} to be used by this {@link Server}.
   *
   * @param eventLoopGroup event loop group to use.
   * @param channelClass channel class to use, should be compatible with the event loop.
   * @return Builder that is set up with a {@link ServerBootstrap}.
   */
  public static Server.Builder<SimulacronCluster> builder(
      EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> channelClass) {
    return Server.builder(eventLoopGroup, channelClass, SimulacronCluster::cbuilder);
  }
}
