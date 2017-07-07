package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.request.*;
import com.datastax.oss.protocol.internal.response.*;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.stubbing.CloseType;
import com.datastax.simulacron.common.utils.FrameUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.local.LocalServerChannel;
import org.junit.After;
import org.junit.Test;

import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static com.datastax.simulacron.server.AddressResolver.localAddressResolver;
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

    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {
      assertThat(boundNode).isInstanceOf(BoundNode.class);
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);
      }
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
    try (BoundCluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

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
          try (MockClient client = new MockClient(eventLoop)) {
            client.connect(node.getAddress());
            client.write(new Startup());
            // Expect a Ready response.
            Frame response = client.next();
            assertThat(response.message).isInstanceOf(Ready.class);
          }
        }
      }
    }
  }

  @Test
  public void testRegisterClusterFailsWhenNodeAlreadyBound() throws Exception {
    Cluster cluster = Cluster.builder().build();
    DataCenter dc = cluster.addDataCenter().build();
    SocketAddress address = localAddressResolver.get();

    // Create 2 nodes with the same address, this should cause issue since both can't be
    // bound to same interface.
    Node node0 = dc.addNode().withAddress(address).build();
    Node node1 = dc.addNode().withAddress(address).build();

    BoundCluster boundCluster = null;
    try {
      boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS);
      fail();
    } catch (Exception e) {
      assertThat(e.getCause()).isInstanceOf(BindNodeException.class);
      BindNodeException bne = (BindNodeException) e.getCause();
      assertThat(bne.getAddress()).isSameAs(address);
      assertThat(bne.getNode()).isIn(node0, node1);
      if (boundCluster != null) {
        assertThat(localServer.getCluster(boundCluster.getId())).isNull();
      }
    }

    // Cluster should not have been registered.
    assertThat(localServer.getClusters()).isEmpty();
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
    Cluster cluster = Cluster.builder().build();
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
  public void testUnregisterCluster() throws Exception {
    Cluster cluster = Cluster.builder().withNodes(2, 2).build();
    try (BoundCluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

      // Should be 4 nodes total.
      List<BoundNode> nodes = boundCluster.getBoundNodes().collect(Collectors.toList());
      assertThat(nodes).hasSize(4);
      for (BoundNode node : nodes) {
        // Each node's channel should be open.
        assertThat(node.channel.get().isOpen()).isTrue();
      }

      try (MockClient client = new MockClient(eventLoop).connect(nodes.get(0).getAddress())) {
        // Use a client, this makes sure that the client channel is initialized and added to channel group.
        // Usually this will be the case, but in some constrained environments there may be a window where
        // the unregister happens before the channel is added to the channel group.
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        // Unregistering the cluster should close each nodes channel and remove cluster.
        assertThat(localServer.unregister(boundCluster).get(5, TimeUnit.SECONDS))
            .isSameAs(boundCluster);

        // Cluster should be removed from registry.
        assertThat(localServer.getCluster(boundCluster.getId())).isNull();

        // All node's channels should be closed.
        for (BoundNode node : nodes) {
          // Each node's channel should be open.
          assertThat((node).channel.get().isOpen()).isFalse();
        }

        // Channel should be closed.  Send a write so client probes connection status (otherwise it may not get close
        // notification right away).
        try {
          ChannelFuture future = client.write(new Startup());
          future.get(5, TimeUnit.SECONDS);
          fail("Expected ClosedChannelException");
        } catch (ExecutionException e) {
          assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
        }
      }
    }
  }

  @Test
  public void testUnregisterClusterWithoutId() throws Exception {
    // attempting to unregister using a Cluster without an assigned ID should thrown an exception.
    Cluster cluster = Cluster.builder().withNodes(2, 2).build();
    try {
      localServer.unregister(cluster).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testUnregisterClusterNotRegistered() throws Exception {
    // attemping to unregister a Cluster that is not registered should throw an exception.
    Cluster cluster = Cluster.builder().withId(Long.MAX_VALUE).withNodes(1).build();
    try {
      localServer.unregister(cluster).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
      assertThat(localServer.getCluster(cluster.getId())).isNull();
    }
  }

  @Test
  public void testUnregisterNodeWithoutCluster() throws Exception {
    // attempting to unregister a Node that has no parent cluster should throw an exception.
    Node node = Node.builder().build();
    try {
      localServer.unregister(node).get(5, TimeUnit.SECONDS);
      fail();
    } catch (ExecutionException ex) {
      assertThat(ex.getCause()).isInstanceOf(IllegalArgumentException.class);
    }
  }

  @Test
  public void testShouldCloseNodeConnections() throws Exception {
    try (BoundNode node = localServer.register(Node.builder()).get(5, TimeUnit.SECONDS);
        MockClient client = new MockClient(eventLoop)) {
      client.connect(node.getAddress());
      client.write(new Startup());

      // Retrieve active connection
      NodeConnectionReport report = node.getConnections();
      assertThat(report.getConnections()).hasSize(1);

      // Close connection
      report = node.closeConnections(CloseType.DISCONNECT).get(5, TimeUnit.SECONDS);
      assertThat(report.getActiveConnections()).isEqualTo(1);

      report = node.getConnections();
      assertThat(report.getConnections()).hasSize(0);
    }
  }

  @Test
  public void testShouldCloseClusterConnections() throws Exception {
    try (BoundCluster cluster =
            localServer.register(Cluster.builder().withNodes(3)).get(5, TimeUnit.SECONDS);
        MockClient client = new MockClient(eventLoop)) {
      client.connect(cluster.node(0, 1).getAddress());
      client.write(new Startup());

      // Retrieve active connection
      ClusterConnectionReport report = cluster.getConnections();
      assertThat(report.getActiveConnections()).isEqualTo(1);

      // Close connection
      report = cluster.closeConnections(CloseType.DISCONNECT).get(5, TimeUnit.SECONDS);
      assertThat(report.getActiveConnections()).isEqualTo(1);

      report = cluster.getConnections();
      assertThat(report.getActiveConnections()).isEqualTo(0);
    }
  }

  @Test
  public void testShouldCloseDataCenterConnections() throws Exception {
    try (BoundCluster cluster =
            localServer.register(Cluster.builder().withNodes(3, 1)).get(5, TimeUnit.SECONDS);
        MockClient client = new MockClient(eventLoop)) {
      client.connect(cluster.node(1, 0).getAddress());
      client.write(new Startup());

      // Retrieve active connection - dc1 should have 1
      DataCenterConnectionReport report = cluster.dc(1).getConnections();
      assertThat(report.getActiveConnections()).isEqualTo(1);

      // Retrieve active connections - dc0 should have 0
      report = cluster.dc(0).getConnections();
      assertThat(report.getActiveConnections()).isEqualTo(0);

      // Close connection
      report = cluster.dc(1).closeConnections(CloseType.DISCONNECT).get(5, TimeUnit.SECONDS);
      assertThat(report.getActiveConnections()).isEqualTo(1);

      report = cluster.dc(1).getConnections();
      assertThat(report.getActiveConnections()).isEqualTo(0);
    }
  }

  @Test
  public void testStopAndStart() throws Exception {
    try (BoundNode boundNode = localServer.register(Node.builder()).get(5, TimeUnit.SECONDS);
        MockClient client = new MockClient(eventLoop)) {
      client.connect(boundNode.getAddress());
      client.write(new Startup());

      NodeConnectionReport report = boundNode.getConnections();
      assertThat(report.getConnections()).hasSize(1);

      // stop the node, connection should close.
      boundNode.stop().get(5, TimeUnit.SECONDS);
      report = boundNode.getConnections();
      assertThat(report.getConnections()).hasSize(0);

      // attempt to connect should fail
      try {
        client.connect(boundNode.getAddress());
        fail("Should not have been able to connect");
      } catch (ConnectException ce) {
        // expected
      }

      // start the node
      boundNode.start().get(5, TimeUnit.SECONDS);

      // attempt to connect should succeed
      client.connect(boundNode.getAddress());

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);

      report = boundNode.getConnections();
      assertThat(report.getConnections()).hasSize(1);
    }
  }

  @Test
  public void testShouldStopAcceptingStartupAndAcceptAgain() throws Exception {
    Node node = Node.builder().build();
    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.REJECT_STARTUP).get(5, TimeUnit.SECONDS);

        // client should remain connected.
        assertThat(client.channel.isOpen()).isTrue();

        // New client should open connection, but fail to get response to startup since not listening.
        try (MockClient client2 = new MockClient(eventLoop)) {
          client2.connect(boundNode.getAddress());
          client2.write(new Startup());
          // Expect a Ready response.
          response = client2.nextQuick();
          assertThat(response).isNull();
        }

        // Start accepting new connections again.
        boundNode.acceptConnections().get(5, TimeUnit.SECONDS);

        // New client should open connection and receive 'Ready' to 'Startup' request.
        try (MockClient client3 = new MockClient(eventLoop)) {
          client3.connect(boundNode.getAddress());
          client3.write(new Startup());
          // Expect a Ready response.
          response = client3.next();
          assertThat(response.message).isInstanceOf(Ready.class);
        }
      }
    }
  }

  @Test
  public void testShouldStopAcceptingConnectionsAndAcceptAgain() throws Exception {
    Node node = Node.builder().build();
    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {

      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.UNBIND).get(5, TimeUnit.SECONDS);

        // client should remain connected.
        assertThat(client.channel.isOpen()).isTrue();

        // New client should not be able to open connection
        try (MockClient client2 = new MockClient(eventLoop)) {
          try {
            client2.connect(boundNode.getAddress());
            fail("Did not expect to be able to connect");
          } catch (ConnectException ce) { // Expected
          }
        }

        // Start accepting new connections again.
        boundNode.acceptConnections().get(5, TimeUnit.SECONDS);

        // New client should open connection and receive 'Ready' to 'Startup' request.
        try (MockClient client3 = new MockClient(eventLoop)) {
          client3.connect(boundNode.getAddress());
          client3.write(new Startup());
          // Expect a Ready response.
          response = client3.next();
          assertThat(response.message).isInstanceOf(Ready.class);
        }
      }
    }
  }

  @Test
  public void testShouldCloseExistingConnectionsAndAcceptAgain() throws Exception {
    Node node = Node.builder().build();
    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.STOP).get(5, TimeUnit.SECONDS);

        // client should not remain connected.
        assertThat(client.channel.isOpen()).isFalse();

        // New client should not be able to open connection
        try (MockClient client2 = new MockClient(eventLoop)) {
          try {
            client2.connect(boundNode.getAddress());
            fail("Did not expect to be able to connect");
          } catch (ConnectException ce) { // Expected
          }
        }

        // Start accepting new connections again.
        boundNode.acceptConnections().get(5, TimeUnit.SECONDS);

        // New client should open connection and receive 'Ready' to 'Startup' request.
        try (MockClient client3 = new MockClient(eventLoop)) {
          client3.connect(boundNode.getAddress());
          client3.write(new Startup());
          // Expect a Ready response.
          response = client3.next();
          assertThat(response.message).isInstanceOf(Ready.class);
        }
      }
    }
  }

  @Test
  public void testShouldStopAcceptingConnectionsAfter5() throws Exception {
    Node node = Node.builder().build();
    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(5, RejectScope.UNBIND).get(5, TimeUnit.SECONDS);

        // client should remain connected.
        assertThat(client.channel.isOpen()).isTrue();

        // open 5 connections, they should all be successful
        for (int i = 0; i < 5; i++) {
          try (MockClient client2 = new MockClient(eventLoop)) {
            client2.connect(boundNode.getAddress());
            client2.write(new Startup());
            // Expect a Ready response.
            response = client2.next();
            assertThat(response.message).isInstanceOf(Ready.class);
          }
        }

        // on 5th attempt listener should be unbound, so new connections should not work.
        try (MockClient client2 = new MockClient(eventLoop)) {
          try {
            client2.connect(boundNode.getAddress());
            fail("Did not expect to be able to connect");
          } catch (ConnectException ce) { // Expected
          }
        }
      }
    }
  }

  @Test
  public void testShouldReturnProtocolErrorWhenUsingUnsupportedProtocolVersion() throws Exception {
    // If connecting with a newer protocol version than simulacron supports, a protocol error should be sent back.
    Node node = Node.builder().build();
    try (BoundNode boundNode = localServer.register(node).get(5, TimeUnit.SECONDS)) {

      // Create encoders/decoders for protocol v6.
      FrameCodec.CodecGroup v6Codecs =
          registry -> {
            registry
                .addEncoder(new AuthResponse.Codec(6))
                .addEncoder(new Batch.Codec(6))
                .addEncoder(new Execute.Codec(6))
                .addEncoder(new Options.Codec(6))
                .addEncoder(new Prepare.Codec(6))
                .addEncoder(new Query.Codec(6))
                .addEncoder(new Register.Codec(6))
                .addEncoder(new Startup.Codec(6));

            registry
                .addDecoder(new AuthChallenge.Codec(6))
                .addDecoder(new Authenticate.Codec(6))
                .addDecoder(new AuthSuccess.Codec(6))
                .addDecoder(new com.datastax.oss.protocol.internal.response.Error.Codec(6))
                .addDecoder(new Event.Codec(6))
                .addDecoder(new Ready.Codec(6))
                .addDecoder(new Result.Codec(6))
                .addDecoder(new Supported.Codec(6));
          };

      FrameCodec<ByteBuf> frameCodec =
          new FrameCodec<>(
              new ByteBufCodec(),
              Compressor.none(),
              new ProtocolV3ClientCodecs(),
              new ProtocolV4ClientCodecs(),
              new ProtocolV5ClientCodecs(),
              v6Codecs);

      try (MockClient client = new MockClient(eventLoop, frameCodec)) {
        client.connect(boundNode.getAddress());
        // Write a startup message with protocol version 6 (which is not supported)
        client.write(
            new Frame(
                6,
                false,
                0,
                false,
                null,
                FrameUtils.emptyCustomPayload,
                Collections.emptyList(),
                new Startup()));

        // Expect a protocol error indicating invalid protocol version.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Error.class);
        assertThat(response.protocolVersion).isEqualTo(4);
        Error err = (Error) response.message;
        assertThat(err.code).isEqualTo(ProtocolConstants.ErrorCode.PROTOCOL_ERROR);
        assertThat(err.message).isEqualTo("Invalid or unsupported protocol version");

        // Try again with protocol version 4, which is supported, this should work on the same connection
        // since the previous message was simply discarded.
        client.write(new Startup());

        // Expect a Ready response.
        response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);
      }
    }
  }

  @Test
  public void testClusterActiveConnections() throws Exception {
    Cluster cluster = Cluster.builder().withNodes(5).build();
    try (BoundCluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS)) {
      List<Node> nodes = boundCluster.getNodes();

      // Create clients and ensure active connections on each node, data center, and cluster
      List<MockClient> clients = new ArrayList<>();
      for (int i = 0; i < nodes.size(); ++i) {
        Node node = nodes.get(i);
        DataCenter dc = node.getDataCenter();
        assertThat(node.getActiveConnections()).isEqualTo(0L);
        assertThat(dc.getActiveConnections()).isEqualTo(i);
        assertThat(boundCluster.getActiveConnections()).isEqualTo(i);

        // Connect to the node
        MockClient client = new MockClient(eventLoop);
        clients.add(client);
        client.connect(node.getAddress());
        client.write(new Startup());
        assertThat(client.next().message).isInstanceOf(Ready.class);

        // Ensure the active connections
        assertThat(node.getActiveConnections()).isEqualTo(1L);
        assertThat(dc.getActiveConnections()).isEqualTo(i + 1);
        assertThat(boundCluster.getActiveConnections()).isEqualTo(i + 1);
      }

      // Close the client connections and ensure the active connections
      for (int i = 0; i < clients.size(); ++i) {
        MockClient client = clients.get(i);
        Node node = nodes.get(i);
        DataCenter dc = node.getDataCenter();

        // Ensure the active connections after disconnect
        client.close();
        // sleep a little bit as disconnected channels may not be unregistered immediately.
        Thread.sleep(50);
        assertThat(node.getActiveConnections()).isEqualTo(0L);
        assertThat(dc.getActiveConnections()).isEqualTo(clients.size() - (i + 1));
        assertThat(boundCluster.getActiveConnections()).isEqualTo(clients.size() - (i + 1));
      }
    }
  }

  @Test
  public void testClusterActiveConnectionsMultipleDataCenters() throws Exception {
    Cluster cluster = Cluster.builder().withNodes(1, 3, 5).build();
    try (BoundCluster boundCluster = localServer.register(cluster).get(5, TimeUnit.SECONDS)) {
      List<Node> nodes = boundCluster.getNodes();

      // Create clients and ensure active connections on each node, data center, and cluster
      List<MockClient> clients = new ArrayList<>();
      for (int i = 0; i < nodes.size(); ++i) {
        Node node = nodes.get(i);
        DataCenter dc = node.getDataCenter();

        // Offset mechanism for determining active connections in data center
        Long activeConnectionsOffset = 0L;
        if (dc.getId() == 1) {
          activeConnectionsOffset = 1L;
        } else if (dc.getId() == 2) {
          activeConnectionsOffset = 4L;
        }

        // Ensure default assertions for node, data center, and cluster
        assertThat(node.getActiveConnections()).isEqualTo(0L);
        assertThat(dc.getActiveConnections()).isEqualTo(i - activeConnectionsOffset);
        assertThat(boundCluster.getActiveConnections()).isEqualTo(i);

        // Connect to the node
        MockClient client = new MockClient(eventLoop);
        clients.add(client);
        client.connect(node.getAddress());
        client.write(new Startup());
        assertThat(client.next().message).isInstanceOf(Ready.class);

        // Ensure the active connections
        assertThat(node.getActiveConnections()).isEqualTo(1L);
        assertThat(dc.getActiveConnections()).isEqualTo((i - activeConnectionsOffset) + 1);
        assertThat(boundCluster.getActiveConnections()).isEqualTo(i + 1);
      }

      // Close the client connections and ensure the active connections
      for (int i = 0; i < clients.size(); ++i) {
        MockClient client = clients.get(i);
        Node node = nodes.get(i);
        DataCenter dc = node.getDataCenter();

        // Offset mechanism for determining active connections in data center
        Long activeConnectionsOffset = 9L;
        if (dc.getId() == 1) {
          activeConnectionsOffset = 6L;
        } else if (dc.getId() == 2) {
          activeConnectionsOffset = 1L;
        }

        // Ensure the active connections after disconnect
        client.close();
        // sleep a little bit as disconnected channels may not be unregistered immediately.
        Thread.sleep(50);
        assertThat(node.getActiveConnections()).isEqualTo(0L);
        assertThat(dc.getActiveConnections())
            .isEqualTo(clients.size() - (activeConnectionsOffset + i));
        assertThat(boundCluster.getActiveConnections()).isEqualTo(clients.size() - (i + 1));
      }
    }
  }

  @Test
  public void testTryWithResourcesShouldCloseCluster() throws Exception {
    Long clusterId;
    SocketAddress address;
    // Validate that when a cluster is created in try-with-resources that when leaving try block
    // that the cluster is unregistered from the server.
    try (BoundCluster cluster =
        localServer.register(Cluster.builder().withNodes(3)).get(5, TimeUnit.SECONDS)) {
      clusterId = cluster.getId();
      address = cluster.node(0, 1).getAddress();
      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(address);
      }
    }

    // ensure cluster id was set.
    assertThat(localServer.getCluster(clusterId)).isNull();
    try (MockClient client = new MockClient(eventLoop)) {
      try {
        client.connect(address);
        fail("Should not have been able to connect");
      } catch (ConnectException ce) {
        // expected
      }
    }
  }

  @Test
  public void testTryWithResourcesShouldCloseNodesCluster() throws Exception {
    Long clusterId;
    SocketAddress address;
    // Validate that when a node is created in try-with-resources that when leaving try block
    // that the associated cluster is unregistered from the server.
    try (BoundNode node = localServer.register(Node.builder()).get(5, TimeUnit.SECONDS)) {
      clusterId = node.getCluster().getId();
      address = node.getAddress();
      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(address);
      }
    }

    // ensure cluster id was unregistered.
    assertThat(localServer.getCluster(clusterId)).isNull();
    try (MockClient client = new MockClient(eventLoop)) {
      try {
        client.connect(address);
        fail("Should not have been able to connect");
      } catch (ConnectException ce) {
        // expected
      }
    }
  }
}
