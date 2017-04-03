package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

public final class Server {

  private static Logger logger = LoggerFactory.getLogger(Server.class);

  private static final AttributeKey<BoundNode> HANDLER = AttributeKey.valueOf("NODE");

  private ServerBootstrap serverBootstrap;

  private static final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultServer(new ByteBufCodec(), Compressor.none());

  final Map<UUID, Cluster> clusters = new ConcurrentHashMap<>();

  final Map<UUID, Node> nodes = new ConcurrentHashMap<>();

  public Server() {
    this(new NioEventLoopGroup(), NioServerSocketChannel.class);
  }

  Server(EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> channelClass) {
    this.serverBootstrap =
        new ServerBootstrap()
            .group(eventLoopGroup)
            .channel(channelClass)
            .childHandler(new Initializer());
  }

  public CompletableFuture<Cluster> register(Cluster cluster) {
    // TODO make a Cluster.copy that doesn't inherit DCs?  DC that doesn't inherit nodes? etc.
    UUID clusterId = UUID.randomUUID();
    Cluster c =
        Cluster.builder()
            .withCassandraVersion(cluster.getCassandraVersion())
            .withPeerInfo(cluster.getPeerInfo())
            .withName(cluster.getName())
            .withId(clusterId)
            .build();

    List<CompletableFuture<Node>> bindFutures = new ArrayList<>();

    for (DataCenter dataCenter : cluster.getDataCenters()) {
      UUID dcId = UUID.randomUUID();
      DataCenter dc =
          DataCenter.builder(c)
              .withCassandraVersion(dataCenter.getCassandraVersion())
              .withPeerInfo(dataCenter.getPeerInfo())
              .withName(dataCenter.getName())
              .withId(dcId)
              .build();

      for (Node node : dataCenter.getNodes()) {
        bindFutures.add(bindInternal(node, dc));
      }
    }

    // TODO: Unbind everything onm failures
    return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
        .thenApply(
            __ -> {
              clusters.put(clusterId, c);
              return c;
            });
  }

  public CompletableFuture<Node> bind(Node node) {
    return bindInternal(node, null);
  }

  private CompletableFuture<Node> bindInternal(Node refNode, DataCenter parent) {
    UUID nodeId = UUID.randomUUID();
    SocketAddress address =
        refNode.getAddress() != null
            ? refNode.getAddress()
            : AddressResolver.defaultResolver.apply(null);
    CompletableFuture<Node> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(refNode.getAddress());
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              BoundNode node =
                  new BoundNode(
                      address,
                      refNode.getName(),
                      nodeId,
                      refNode.getCassandraVersion(),
                      refNode.getPeerInfo(),
                      parent,
                      channelFuture.channel());
              logger.info("Bound {} to {}", node, channelFuture.channel());
              channelFuture.channel().attr(HANDLER).set(node);
              if (parent == null) {
                nodes.put(nodeId, node);
              }
              f.complete(node);
            });

    return f;
  }

  static class RequestHandler extends ChannelInboundHandlerAdapter {

    private BoundNode node;
    private FrameCodec<ByteBuf> frameCodec;

    private RequestHandler(BoundNode node, FrameCodec<ByteBuf> frameCodec) {
      this.node = node;
      this.frameCodec = frameCodec;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      MDC.put("node", node.getId().toString());

      try {
        @SuppressWarnings("unchecked")
        Frame frame = (Frame) msg;
        node.handle(ctx, frame, frameCodec);
      } finally {
        MDC.remove("node");
      }
    }
  }

  static class Initializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      BoundNode node = channel.parent().attr(HANDLER).get();
      MDC.put("node", node.getId().toString());

      try {
        logger.info("Got new connection {}", channel);

        pipeline
            .addLast("decoder", new FrameDecoder(frameCodec))
            .addLast("requestHandler", new RequestHandler(node, frameCodec));
      } finally {
        MDC.remove("node");
      }
    }
  }
}
