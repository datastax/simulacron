package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.simulacron.cluster.Cluster;
import com.datastax.simulacron.cluster.Node;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public final class Server {

  private static Logger logger = LoggerFactory.getLogger(Server.class);

  private static final AttributeKey<Node> HANDLER = AttributeKey.valueOf("NODE");

  private ServerBootstrap serverBootstrap;

  private static final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultServer(new ByteBufCodec(), Compressor.none());

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

  public CompletableFuture<Cluster> bind(Cluster cluster) {
    // TODO, need a means of binding/unbinding cluster and nodes, thus returning cluster/node is not appropriate
    List<CompletableFuture<Node>> bindFutures =
        cluster
            .dataCenters()
            .stream()
            .flatMap(dc -> dc.nodes().stream())
            .map(this::bind)
            .collect(Collectors.toList());

    return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
        .thenApply(__ -> cluster);
  }

  public CompletableFuture<Node> bind(Node node) {
    CompletableFuture<Node> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(node.address());
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              logger.info("Bound {} to {}", node, channelFuture.channel());
              channelFuture.channel().attr(HANDLER).set(node);
              f.complete(node);
            });
    return f;
  }

  static class RequestHandler extends ChannelInboundHandlerAdapter {

    private Node node;
    private FrameCodec<ByteBuf> frameCodec;

    private RequestHandler(Node node, FrameCodec<ByteBuf> frameCodec) {
      this.node = node;
      this.frameCodec = frameCodec;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      MDC.put("node", node.id().toString());

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
      Node node = channel.parent().attr(HANDLER).get();
      MDC.put("node", node.id().toString());

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
