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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public final class Server {

  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  private static final AttributeKey<BoundNode> HANDLER = AttributeKey.valueOf("NODE");

  private static final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultServer(new ByteBufCodec(), Compressor.none());

  private ServerBootstrap serverBootstrap;

  private final AddressResolver addressResolver;

  private final long bindTimeoutInNanos;

  final Map<UUID, Cluster> clusters = new ConcurrentHashMap<>();

  private Server(
      AddressResolver addressResolver, ServerBootstrap serverBootstrap, long bindTimeoutInNanos) {
    this.serverBootstrap = serverBootstrap;
    this.addressResolver = addressResolver;
    this.bindTimeoutInNanos = bindTimeoutInNanos;
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

    List<BoundNode> nodes = new ArrayList<>(bindFutures.size());
    Throwable exception = null;
    boolean timedOut = false;
    // Evaluate each future, if any fail capture the exception and record all successfully
    // bound nodes.
    long start = System.nanoTime();
    // TODO find way to test timeout scenarios, guess would have to mock the ServerBootstrap somehow?
    for (CompletableFuture<Node> f : bindFutures) {
      try {
        if (timedOut) {
          Node node = f.getNow(null);
          if (node != null) {
            nodes.add((BoundNode) node);
          }
        } else {
          nodes.add((BoundNode) f.get(bindTimeoutInNanos, TimeUnit.NANOSECONDS));
        }
        if (System.nanoTime() - start > bindTimeoutInNanos) {
          timedOut = true;
        }
      } catch (TimeoutException te) {
        timedOut = true;
        exception = te;
      } catch (Exception e) {
        exception = e.getCause();
      }
    }

    // If there was a failure close all bound nodes.
    if (exception != null) {
      final Throwable e = exception;
      // Close each node
      List<CompletableFuture<Node>> futures =
          nodes.stream().map(this::close).collect(Collectors.toList());

      CompletableFuture<Cluster> future = new CompletableFuture<>();
      // On completion of unbinding all nodes, fail the returned future.
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
          .handle(
              (v, ex) -> {
                if (ex != null) {
                  logger.warn("Exception while unbinding nodes", ex);
                }
                future.completeExceptionally(e);
                return v;
              });
      return future;
    } else {
      return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
          .thenApply(__ -> c);
    }
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
            BoundNode boundNode = (BoundNode) node;
            closeFutures.add(close(boundNode));
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
    Cluster cluster = node.getCluster();
    if (cluster == null) {
      CompletableFuture<Node> future = new CompletableFuture<>();
      future.completeExceptionally(
          new IllegalArgumentException("Node has no Cluster, must not be bound."));
      return future;
    } else {
      return unregister(cluster).thenApply(__ -> node);
    }
  }

  private CompletableFuture<Node> bindInternal(Node refNode, DataCenter parent) {
    UUID nodeId = UUID.randomUUID();
    // Use node's address if set, otherwise generate a new one.
    SocketAddress address =
        refNode.getAddress() != null ? refNode.getAddress() : addressResolver.get();
    CompletableFuture<Node> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(address);
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              if (channelFuture.isSuccess()) {
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
              } else {
                // If failed, propagate it.
                f.completeExceptionally(channelFuture.cause());
              }
            });

    return f;
  }

  private CompletableFuture<Node> close(BoundNode node) {
    CompletableFuture<Node> future = new CompletableFuture<>();
    node.channel
        .close()
        .addListener(
            channelFuture -> {
              future.complete(node);
            });
    return future;
  }

  public static Builder builder() {
    return builder(new NioEventLoopGroup(), NioServerSocketChannel.class);
  }

  public static Builder builder(
      EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> channelClass) {
    ServerBootstrap serverBootstrap =
        new ServerBootstrap()
            .group(eventLoopGroup)
            .channel(channelClass)
            .childHandler(new Initializer());
    return new Builder(serverBootstrap);
  }

  static class Builder {
    private ServerBootstrap serverBootstrap;

    private AddressResolver addressResolver = AddressResolver.defaultResolver;

    private static long DEFAULT_BIND_TIMEOUT_IN_NANOS =
        TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    private long bindTimeoutInNanos = DEFAULT_BIND_TIMEOUT_IN_NANOS;

    Builder(ServerBootstrap initialBootstrap) {
      this.serverBootstrap = initialBootstrap;
    }

    public Builder withBindTimeout(long time, TimeUnit timeUnit) {
      this.bindTimeoutInNanos = TimeUnit.NANOSECONDS.convert(time, timeUnit);
      return this;
    }

    public Builder withAddressResolver(AddressResolver addressResolver) {
      this.addressResolver = addressResolver;
      return this;
    }

    public Server build() {
      return new Server(addressResolver, serverBootstrap, bindTimeoutInNanos);
    }
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
