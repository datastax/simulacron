package com.datastax.simulacron;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.concurrent.CompletableFuture;

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

  public CompletableFuture<Void> bind(Node node) {
    CompletableFuture<Void> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(node.address(), node.port());
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              channelFuture.channel().attr(HANDLER).set(node);
              f.complete(null);
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
      MDC.put("node", node.toString());

      try {
        @SuppressWarnings("unchecked")
        Frame frame = (Frame) msg;
        node.handle(ctx, frame, frameCodec);
      } finally {
        MDC.remove("node");
      }
    }
  }

  static class Initializer extends ChannelInitializer<SocketChannel> {

    @Override
    protected void initChannel(SocketChannel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      Node node = channel.parent().attr(HANDLER).get();
      MDC.put("node", node.toString());

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
