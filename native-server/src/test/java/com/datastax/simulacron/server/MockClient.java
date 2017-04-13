package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.datastax.simulacron.common.utils.FrameUtils.wrapRequest;

public class MockClient implements Closeable {

  // Set up client bootstrap that interacts with server
  Bootstrap cb = new Bootstrap();

  BlockingQueue<Frame> responses = new LinkedBlockingQueue<>();

  FrameCodec<ByteBuf> frameCodec = FrameCodec.defaultClient(new ByteBufCodec(), Compressor.none());

  private Channel channel;

  MockClient(EventLoopGroup elg) {
    // Set up so written Frames are encoded into bytes, received bytes are encoded into Frames put on queue.
    cb.group(elg)
        .channel(LocalChannel.class)
        .handler(
            new ChannelInitializer<LocalChannel>() {
              @Override
              protected void initChannel(LocalChannel ch) throws Exception {
                ch.pipeline()
                    .addLast(new FrameEncoder(frameCodec))
                    .addLast(new FrameDecoder(frameCodec))
                    .addLast(
                        new ChannelInboundHandlerAdapter() {
                          @Override
                          public void channelRead(ChannelHandlerContext ctx, Object msg)
                              throws Exception {
                            responses.offer((Frame) msg);
                          }
                        });
              }
            });
  }

  MockClient connect(SocketAddress address) throws Exception {
    if (channel == null) {
      this.channel = cb.connect(address).sync().channel();
    }
    return this;
  }

  void write(Message message) {
    write(wrapRequest(message));
  }

  void write(Frame frame) {
    this.channel.writeAndFlush(frame);
  }

  Frame next() throws InterruptedException {
    return responses.poll(5, TimeUnit.SECONDS);
  }

  @Override
  public void close() throws IOException {
    try {
      this.channel.close().sync();
    } catch (InterruptedException e) {
      //no op
    }
  }
}
