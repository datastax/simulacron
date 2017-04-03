package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.Node;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.datastax.simulacron.server.FrameUtils.wrapRequest;
import static org.assertj.core.api.Assertions.assertThat;

public class ServerTest {

  @Test
  public void testRegisterNode() throws Exception {
    // Use a LocalAddress to use VM pipes
    EventLoopGroup elg = new DefaultEventLoopGroup();
    UUID nodeId = UUID.randomUUID();
    SocketAddress address = AddressResolver.localAddressResolver.apply(nodeId.toString());
    Server server = new Server(elg, LocalServerChannel.class);
    Node node = Node.builder().withId(nodeId).withAddress(address).build();

    // Should bind within 5 seconds and get a bound node back.
    Node boundNode = server.register(node).get(5, TimeUnit.SECONDS);
    assertThat(boundNode).isInstanceOf(BoundNode.class);

    // Should be wrapped and registered in a dummy cluster.
    assertThat(server.clusters.get(boundNode.getCluster().getId()))
        .isSameAs(boundNode.getCluster());

    try (Client client = new Client(elg)) {
      client.connect(address);
      client.write(new Startup());
      // Expect a Ready response.
      Frame response = client.next();
      assertThat(response.message).isInstanceOf(Ready.class);
    }
  }

  @Test
  public void testUnregisterNode() throws Exception {
    // Bind node and ensure channel is open.
    EventLoopGroup elg = new DefaultEventLoopGroup();
    UUID nodeId = UUID.randomUUID();
    SocketAddress address = AddressResolver.localAddressResolver.apply(nodeId.toString());
    Server server = new Server(elg, LocalServerChannel.class);
    Node node = Node.builder().withId(nodeId).withAddress(address).build();
    BoundNode boundNode = (BoundNode) server.register(node).get(5, TimeUnit.SECONDS);

    assertThat(boundNode.channel.isOpen()).isTrue();

    // Wrapper cluster should be registered.
    Cluster cluster = boundNode.getCluster();
    assertThat(server.clusters).containsKey(cluster.getId());

    // Unregistering the node should close the nodes channel and remove cluster.
    assertThat(server.unregister(boundNode).get(5, TimeUnit.SECONDS)).isSameAs(boundNode);

    // Channel should be closed.
    assertThat(boundNode.channel.isOpen()).isFalse();
  }

  public static class Client implements Closeable {

    // Set up client bootstrap that interacts with server
    Bootstrap cb = new Bootstrap();

    BlockingQueue<Frame> responses = new LinkedBlockingQueue<>();

    FrameCodec<ByteBuf> frameCodec =
        FrameCodec.defaultClient(new ByteBufCodec(), Compressor.none());

    private Channel channel;

    Client(EventLoopGroup elg) {
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

    Client connect(SocketAddress address) throws Exception {
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
}
