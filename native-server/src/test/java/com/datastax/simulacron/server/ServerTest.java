package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import org.junit.After;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

import static com.datastax.simulacron.server.AddressResolver.localAddressResolver;
import static com.datastax.simulacron.server.FrameUtils.wrapRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class ServerTest {

  private final EventLoopGroup eventLoop = new DefaultEventLoopGroup();

  private final Server localServer =
      Server.builder(eventLoop, LocalServerChannel.class)
          .withAddressResolver(localAddressResolver)
          .build();

  @After
  public void tearDown() {
    eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
  }

  @Test
  public void testRegisterNode() throws Exception {
    Node node = Node.builder().build();

    // Should bind within 5 seconds and get a bound node back.
    Node boundNode = localServer.register(node).get(5, TimeUnit.SECONDS);
    assertThat(boundNode).isInstanceOf(BoundNode.class);

    // Should be wrapped and registered in a dummy cluster.
    assertThat(localServer.getClusterRegistry().get(boundNode.getCluster().getId()))
        .isSameAs(boundNode.getCluster());

    try (Client client = new Client(eventLoop)) {
      client.connect(boundNode.getAddress());
      client.write(new Startup());
      // Expect a Ready response.
      Frame response = client.next();
      assertThat(response.message).isInstanceOf(Ready.class);
    }
  }

  @Test
  public void testRegisterNodeBelongingToACluster() throws Exception {
    // attempting to register a node on its own that belongs to a cluster should fail.
    Cluster cluster = Cluster.builder().build();
    DataCenter dc = cluster.addDataCenter().build();
    Node node = dc.addNode().build();

    try {
      localServer.register(node).get(5, TimeUnit.SECONDS);
      fail();
    } catch (Exception e) {
      assertThat(e.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testRegisterCluster() throws Exception {
    Cluster cluster = Cluster.builder().withNodes(5, 5).build();
    Cluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS);

    // Cluster should be registered.
    assertThat(localServer.getClusterRegistry().get(boundCluster.getId())).isSameAs(boundCluster);

    // Should be 2 DCs.
    assertThat(boundCluster.getDataCenters()).hasSize(2);
    // Ensure an ID is assigned to each DC and Node.
    for (DataCenter dataCenter : boundCluster.getDataCenters()) {
      // Each DC has 5 nodes.
      assertThat(dataCenter.getNodes()).hasSize(5);
      assertThat(dataCenter.getId()).isNotNull();

      for (Node node : dataCenter.getNodes()) {
        // Each node should have assigned address and has an ID.
        assertThat(node.getAddress()).isNotNull();
        assertThat(node.getId()).isNotNull();

        // Each node should handle data.
        try (Client client = new Client(eventLoop)) {
          client.connect(node.getAddress());
          client.write(new Startup());
          // Expect a Ready response.
          Frame response = client.next();
          assertThat(response.message).isInstanceOf(Ready.class);
        }
      }
    }
  }

  @Test
  public void testRegisterClusterFailsWhenNodeAlreadyBound() throws Exception {
    Cluster cluster = Cluster.builder().withId(UUID.randomUUID()).build();
    DataCenter dc = cluster.addDataCenter().build();
    SocketAddress address = localAddressResolver.get();

    // Create 2 nodes with the same address, this should cause issue since both can't be
    // bound to same interface.
    dc.addNode().withAddress(address).build();
    dc.addNode().withAddress(address).build();

    try {
      localServer.register(cluster).get(5, TimeUnit.SECONDS);
      fail();
    } catch (Exception e) {
      assertThat(e.getCause()).isInstanceOf(ChannelException.class);
      assertThat(localServer.getClusterRegistry()).doesNotContainKey(cluster.getId());
    }
  }

  /** A custom handler that delays binding of a socket by 1 second for the given address. */
  @ChannelHandler.Sharable
  class SlowBindHandler extends ChannelOutboundHandlerAdapter {

    SocketAddress slowAddr;

    SlowBindHandler(SocketAddress slowAddr) {
      this.slowAddr = slowAddr;
    }

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise)
        throws Exception {
      if (localAddress == slowAddr) {
        // delay binding 1 second.
        eventLoop.schedule(
            () -> {
              try {
                super.bind(ctx, localAddress, promise);
              } catch (Exception e) {
                // shouldn't happen.
              }
            },
            1,
            TimeUnit.SECONDS);
      } else {
        super.bind(ctx, localAddress, promise);
      }
    }
  }

  @Test
  public void testRegisterClusterFailsWhenBindTimesOut() throws Exception {
    // Designated address to be slow to bind.
    SocketAddress slowAddr = localAddressResolver.get();

    // create a bootstrap with a handler that delays binding by 1 second for designated address.
    ServerBootstrap serverBootstrap =
        new ServerBootstrap()
            .group(eventLoop)
            .channel(LocalServerChannel.class)
            .handler(new SlowBindHandler(slowAddr))
            .childHandler(new Server.Initializer());

    // Define server with 500ms timeout, which should cause binding of slow address to timeout and fail register.
    Server flakyServer =
        new Server.Builder(serverBootstrap)
            .withAddressResolver(localAddressResolver)
            .withBindTimeout(500, TimeUnit.MILLISECONDS)
            .build();

    // Create a 2 node cluster with 1 node having the slow address.
    Cluster cluster = Cluster.builder().withId(UUID.randomUUID()).build();
    DataCenter dc = cluster.addDataCenter().build();
    dc.addNode().withAddress(slowAddr).build();
    dc.addNode().build();

    // Attempt to register which should fail.
    try {
      flakyServer.register(cluster).get(5, TimeUnit.SECONDS);
      fail();
    } catch (Exception e) {
      // Expect a timeout exception.
      assertThat(e.getCause()).isInstanceOf(TimeoutException.class);
    }
  }

  @Test
  public void testUnregisterNode() throws Exception {
    // Bind node and ensure channel is open.
    Node node = Node.builder().build();
    BoundNode boundNode = (BoundNode) localServer.register(node).get(5, TimeUnit.SECONDS);

    // Channel should be open.
    assertThat(boundNode.channel.isOpen()).isTrue();

    // Wrapper cluster should be registered.
    Cluster cluster = boundNode.getCluster();
    assertThat(localServer.getClusterRegistry()).containsKey(cluster.getId());

    // Unregistering the node should close the nodes channel and remove cluster.
    assertThat(localServer.unregisterNode(boundNode.getId()).get(5, TimeUnit.SECONDS))
        .isSameAs(boundNode);

    // Node's cluster should be removed from registry.
    assertThat(localServer.getClusterRegistry()).doesNotContainKey(cluster.getId());

    // Channel should be closed.
    assertThat(boundNode.channel.isOpen()).isFalse();
  }

  @Test
  public void testUnregisterCluster() throws Exception {
    Cluster cluster = Cluster.builder().withNodes(2, 2).build();
    Cluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS);

    // Cluster should be registered.
    assertThat(localServer.getClusterRegistry().get(boundCluster.getId())).isSameAs(boundCluster);

    // Should be 4 nodes total.
    List<Node> nodes = boundCluster.getNodes();
    assertThat(nodes).hasSize(4);
    for (Node node : nodes) {
      // Each node's channel should be open.
      assertThat(((BoundNode) node).channel.isOpen()).isTrue();
    }

    // Unregistering the cluster should close each nodes channel and remove cluster.
    assertThat(localServer.unregister(boundCluster.getId()).get(5, TimeUnit.SECONDS))
        .isSameAs(boundCluster);

    // Cluster should be removed from registry.
    assertThat(localServer.getClusterRegistry()).doesNotContainKey(boundCluster.getId());

    // All node's channels should be closed.
    for (Node node : nodes) {
      // Each node's channel should be open.
      assertThat(((BoundNode) node).channel.isOpen()).isFalse();
    }
  }

  @Test
  public void testUnregisterClusterWithoutId() throws Exception {
    // attempting to unregister using a Cluster without an assigned ID should thrown an exception.
    Cluster cluster = Cluster.builder().withNodes(2, 2).build();
    try {
      localServer.unregister(cluster.getId()).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testUnregisterClusterNotRegistered() throws Exception {
    // attemping to unregister a Cluster that is not registered should throw an exception.
    Cluster cluster = Cluster.builder().withId(UUID.randomUUID()).withNodes(1).build();
    try {
      localServer.unregister(cluster.getId()).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
      assertThat(localServer.getClusterRegistry()).doesNotContainKey(cluster.getId());
    }
  }

  @Test
  public void testUnregisterNodeWithoutCluster() throws Exception {
    // attempting to unregister a Node that has no parent cluster should throw an exception.
    Node node = Node.builder().build();
    try {
      localServer.unregister(node.getId()).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testUnregisterNodeWithClusterNotRegistered() throws Exception {
    // attempting to unregister a Node whose cluster is not registered should throw an exception.
    Node node = Node.builder().build();
    Node boundNode = localServer.register(node).get(5, TimeUnit.SECONDS);
    localServer.unregisterNode(boundNode.getId()).get(5, TimeUnit.SECONDS);

    try {
      // attempt unregister a second time which should fail since already unregistered.
      localServer.unregisterNode(boundNode.getId()).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testUnregisterNodeWithMultiNodeCluster() throws Exception {
    // attempting to unregister a Node which belongs to a multi-node Cluster should throw an exception.
    Cluster cluster = Cluster.builder().withNodes(1, 1, 1).build();
    Cluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS);

    Node node2 = boundCluster.getNodes().get(1);

    try {
      localServer.unregisterNode(node2.getId()).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
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
