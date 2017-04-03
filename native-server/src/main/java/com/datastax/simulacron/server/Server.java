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
    UUID clusterId = UUID.randomUUID();
    Cluster c = Cluster.builder().copy(cluster).withId(clusterId).build();

    List<CompletableFuture<Node>> bindFutures = new ArrayList<>();

    for (DataCenter dataCenter : cluster.getDataCenters()) {
      UUID dcId = UUID.randomUUID();
      DataCenter dc = DataCenter.builder(c).copy(dataCenter).withId(dcId).build();

      for (Node node : dataCenter.getNodes()) {
        bindFutures.add(bindInternal(node, dc));
      }
    }

    clusters.put(clusterId, c);
    // TODO: Unbind everything on failures
    return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
        .thenApply(__ -> c);
  }

  public CompletableFuture<Cluster> unregister(Cluster cluster) {
    CompletableFuture<Cluster> future = new CompletableFuture<>();
    UUID clusterId = cluster.getId();
    if (clusterId == null) {
      future.completeExceptionally(
          new IllegalArgumentException("Cluster has no id, must not be bound"));
    } else {
      Cluster foundCluster = clusters.get(clusterId);
      List<CompletableFuture<Node>> closeFutures = new ArrayList<>();
      if (foundCluster != null) {
        // Close socket on each node.
        for (DataCenter dataCenter : foundCluster.getDataCenters()) {
          for (Node node : dataCenter.getNodes()) {
            CompletableFuture<Node> closeFuture = new CompletableFuture<>();
            BoundNode boundNode = (BoundNode) node;
            boundNode
                .channel
                .close()
                .addListener(
                    channelFuture -> {
                      closeFuture.complete(boundNode);
                    });
            closeFutures.add(closeFuture);
          }
        }
        CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[] {}))
            .thenAccept(
                __ -> {
                  // remove cluster and complete future.
                  clusters.remove(clusterId);
                  future.complete(foundCluster);
                });
      } else {
        future.completeExceptionally(new IllegalArgumentException("Cluster not found."));
      }
    }

    return future;
  }

  public CompletableFuture<Node> register(Node node) {
    // Wrap node in dummy cluster
    UUID clusterId = UUID.randomUUID();
    Cluster dummyCluster = Cluster.builder().withId(clusterId).withName("dummy").build();
    UUID dcId = UUID.randomUUID();
    DataCenter dummyDataCenter =
        dummyCluster.addDataCenter().withName("dummy").withId(dcId).build();

    clusters.put(clusterId, dummyCluster);

    return bindInternal(node, dummyDataCenter);
  }

  public CompletableFuture<Node> unregister(Node node) {
    CompletableFuture<Node> future = new CompletableFuture<>();
    Cluster cluster = node.getCluster();
    if (cluster == null) {
      future.completeExceptionally(
          new IllegalArgumentException("Node has no Cluster, must not be bound."));
      return future;
    } else {
      unregister(cluster).thenApply(c -> future.complete(node));
    }
    return future;
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
