package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.simulacron.common.cluster.Node;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import org.junit.Ignore;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ServerTest {

  // TODO: Ignore until handle is reimplemented
  @Test
  @Ignore
  public void testBind() throws Exception {
    // Use a LocalAddress to use VM pipes
    EventLoopGroup elg = new DefaultEventLoopGroup();
    UUID nodeId = UUID.randomUUID();
    SocketAddress address = AddressResolver.localAddressResolver.apply(nodeId.toString());
    Server server = new Server(elg, LocalServerChannel.class);
    Node node = Node.builder().withId(nodeId).withAddress(address).build();

    // Should bind within 5 seconds.
    server.bind(node).get(5, TimeUnit.SECONDS);

    FrameCodec<ByteBuf> frameCodec =
        FrameCodec.defaultClient(new ByteBufCodec(), Compressor.none());

    // Set up client bootstrap that interacts with server
    Bootstrap cb = new Bootstrap();
    // Set up so written Frames are encoded into bytes, received bytes are encoded into Frames put on queue.
    BlockingQueue<Frame> responses = new LinkedBlockingQueue<>();
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

    // Create a client connection and write a 'Startup' frame on it.
    Channel ch = cb.connect(address).sync().channel();
    ch.writeAndFlush(
        new Frame(
            4,
            false,
            0,
            false,
            null,
            Collections.emptyMap(),
            Collections.emptyList(),
            new Startup()));

    // Expect a Ready response within 5 seconds.
    Frame response = responses.poll(5, TimeUnit.SECONDS);
    assertThat(response.message).isInstanceOf(Ready.class);
  }
}
