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
package com.datastax.oss.simulacron.server;

import static com.datastax.oss.simulacron.common.utils.FrameUtils.wrapRequest;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalChannel;
import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MockClient implements Closeable {

  // Set up client bootstrap that interacts with server
  Bootstrap cb = new Bootstrap();

  BlockingQueue<Frame> responses = new LinkedBlockingQueue<>();

  Channel channel;

  MockClient(EventLoopGroup elg, FrameCodec<ByteBuf> frameCodec) {
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

  MockClient(EventLoopGroup elg) {
    this(elg, FrameCodec.defaultClient(new ByteBufCodec(), Compressor.none()));
  }

  MockClient connect(SocketAddress address) throws Exception {
    this.channel = cb.connect(address).sync().channel();
    return this;
  }

  ChannelFuture write(Message message) {
    return write(wrapRequest(message));
  }

  ChannelFuture write(Frame frame) {
    return this.channel.writeAndFlush(frame);
  }

  Frame next() throws InterruptedException {
    return responses.poll(5, TimeUnit.SECONDS);
  }

  Frame nextQuick() throws InterruptedException {
    return responses.poll(500, TimeUnit.MILLISECONDS);
  }

  @Override
  public void close() throws IOException {
    try {
      if (this.channel != null) {
        this.channel.close().sync();
      }
    } catch (InterruptedException e) {
      //no op
    }
  }
}
