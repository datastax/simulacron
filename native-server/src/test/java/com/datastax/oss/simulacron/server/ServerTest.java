/*
 * Copyright DataStax, Inc.
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

import static com.datastax.oss.simulacron.server.AddressResolver.localAddressResolver;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.ProtocolV3ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV4ClientCodecs;
import com.datastax.oss.protocol.internal.ProtocolV5ClientCodecs;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.AuthChallenge;
import com.datastax.oss.protocol.internal.response.AuthSuccess;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.Event;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Result;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.EmptyReturnMetadataHandler;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.net.ConnectException;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.assertj.core.util.Lists;
import org.junit.After;
import org.junit.Test;

public class ServerTest {

  private final EventLoopGroup eventLoop = new DefaultEventLoopGroup();

  private final Server localServer =
      Server.builder()
          .withEventLoopGroup(eventLoop, LocalServerChannel.class)
          .withAddressResolver(localAddressResolver)
          .build();

  @After
  public void tearDown() {
    eventLoop.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly();
  }

  @Test
  public void testRegisterNode() throws Exception {
    NodeSpec node = NodeSpec.builder().build();

    try (BoundNode boundNode = localServer.register(node)) {
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
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc = cluster.addDataCenter().build();
    NodeSpec node = dc.addNode().build();

    try {
      localServer.register(node);
      fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testRegisterCluster() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().withNodes(5, 5).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

      // Should be 2 DCs.
      assertThat(boundCluster.getDataCenters()).hasSize(2);
      // Ensure an ID is assigned to each DC and NodeSpec.
      for (BoundDataCenter dataCenter : boundCluster.getDataCenters()) {
        // Each DC has 5 nodes.
        assertThat(dataCenter.getNodes()).hasSize(5);
        assertThat(dataCenter.getId()).isNotNull();

        for (BoundNode node : dataCenter.getNodes()) {
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
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc = cluster.addDataCenter().build();
    SocketAddress address = localAddressResolver.get();

    // Create 2 nodes with the same address, this should cause issue since both can't be
    // bound to same interface.
    NodeSpec node0 = dc.addNode().withAddress(address).build();
    NodeSpec node1 = dc.addNode().withAddress(address).build();

    BoundCluster boundCluster = null;
    try {
      boundCluster = localServer.register(cluster);
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

    // Define server with 500ms timeout, which should cause binding of slow address to timeout and
    // fail register.
    Server flakyServer =
        new Server(
            localAddressResolver,
            eventLoop,
            true,
            new HashedWheelTimer(),
            false,
            TimeUnit.NANOSECONDS.convert(500, TimeUnit.MILLISECONDS),
            new StubStore(),
            false,
            serverBootstrap);

    // Create a 2 node cluster with 1 node having the slow address.
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc = cluster.addDataCenter().build();
    dc.addNode().withAddress(slowAddr).build();
    dc.addNode().build();

    // Attempt to register which should fail.
    try {
      flakyServer.register(cluster);
      fail();
    } catch (Exception e) {
      // Expect a timeout exception.
      assertThat(e.getCause()).isInstanceOf(TimeoutException.class);
    }
  }

  @Test
  public void testUnregisterCluster() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2, 2).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

      // Should be 4 nodes total.
      Collection<BoundNode> nodes = boundCluster.getNodes();
      assertThat(nodes).hasSize(4);
      for (BoundNode node : nodes) {
        // Each node's channel should be open.
        assertThat(node.channel.get().isOpen()).isTrue();
      }

      try (MockClient client =
          new MockClient(eventLoop).connect(boundCluster.node(0).getAddress())) {
        // Use a client, this makes sure that the client channel is initialized and added to channel
        // group. Usually this will be the case, but in some constrained environments there may be a
        // window where the unregister happens before the channel is added to the channel group.
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        // Unregistering the cluster should close each nodes channel and remove cluster.
        assertThat(localServer.unregister(boundCluster)).isSameAs(boundCluster);

        // Cluster should be removed from registry.
        assertThat(localServer.getCluster(boundCluster.getId())).isNull();

        // All node's channels should be closed.
        for (BoundNode node : nodes) {
          // Each node's channel should be open.
          assertThat((node).channel.get().isOpen()).isFalse();
        }

        // Channel should be closed.  Send a write so client probes connection status (otherwise it
        // may not get close notification right away).
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
  public void testShouldCloseNodeConnections() throws Exception {
    try (BoundNode node = localServer.register(NodeSpec.builder());
        MockClient client = new MockClient(eventLoop)) {
      client.connect(node.getAddress());
      client.write(new Startup());

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);

      // Retrieve active connection
      NodeConnectionReport report = node.getConnections();
      assertThat(report.getConnections()).hasSize(1);

      // Close connection
      report = node.closeConnections(CloseType.DISCONNECT);
      assertThat(report.getConnections()).hasSize(1);

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);

      report = node.getConnections();
      assertThat(report.getConnections()).hasSize(0);
    }
  }

  @Test
  public void testShouldCloseClusterConnections() throws Exception {
    try (BoundCluster cluster = localServer.register(ClusterSpec.builder().withNodes(3));
        MockClient client = new MockClient(eventLoop)) {
      client.connect(cluster.node(0, 1).getAddress());
      client.write(new Startup());

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);

      // Retrieve active connection
      ClusterConnectionReport report = cluster.getConnections();
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(1);

      // Close connection
      report = cluster.closeConnections(CloseType.DISCONNECT);
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(1);

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);

      report = cluster.getConnections();
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(0);
    }
  }

  @Test
  public void testShouldCloseDataCenterConnections() throws Exception {
    try (BoundCluster cluster = localServer.register(ClusterSpec.builder().withNodes(3, 1));
        MockClient client = new MockClient(eventLoop)) {
      client.connect(cluster.node(1, 0).getAddress());
      client.write(new Startup());

      // Retrieve active connection - dc1 should have 1
      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);
      DataCenterConnectionReport report = cluster.dc(1).getConnections();
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(1);

      // Retrieve active connections - dc0 should have 0
      report = cluster.dc(0).getConnections();
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(0);

      // Close connection
      report = cluster.dc(1).closeConnections(CloseType.DISCONNECT);
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(1);

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);
      report = cluster.dc(1).getConnections();
      assertThat(report.getNodes().stream().mapToInt(r -> r.getConnections().size()).sum())
          .isEqualTo(0);
    }
  }

  @Test
  public void testStopAndStart() throws Exception {
    try (BoundNode boundNode = localServer.register(NodeSpec.builder());
        MockClient client = new MockClient(eventLoop)) {
      client.connect(boundNode.getAddress());
      client.write(new Startup());

      // sleep a little bit as connected channels may not be registered immediately.
      Thread.sleep(50);
      NodeConnectionReport report = boundNode.getConnections();
      assertThat(report.getConnections()).hasSize(1);

      // stop the node, connection should close.
      boundNode.stop();
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
      boundNode.start();

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
    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = localServer.register(node)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.REJECT_STARTUP);

        // client should remain connected.
        assertThat(client.channel.isOpen()).isTrue();

        // New client should open connection, but fail to get response to startup since not
        // listening.
        try (MockClient client2 = new MockClient(eventLoop)) {
          client2.connect(boundNode.getAddress());
          client2.write(new Startup());
          // Expect a Ready response.
          response = client2.nextQuick();
          assertThat(response).isNull();
        }

        // Start accepting new connections again.
        boundNode.acceptConnections();

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
    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = localServer.register(node)) {

      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.UNBIND);

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
        boundNode.acceptConnections();

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
    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = localServer.register(node)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(-1, RejectScope.STOP);

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
        boundNode.acceptConnections();

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
    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = localServer.register(node)) {
      // Should be wrapped and registered in a dummy cluster.
      assertThat(localServer.getCluster(boundNode.getCluster().getId()))
          .isSameAs(boundNode.getCluster());

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());
        client.write(new Startup());
        // Expect a Ready response.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);

        boundNode.rejectConnections(5, RejectScope.UNBIND);

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
  public void testShouldReturnProtocolErrorWhenUsingUnsupportedProtocolVersionV6()
      throws Exception {
    // If connecting with a newer protocol version than simulacron supports by default, a protocol
    // error should be sent back.
    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = localServer.register(node)) {

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
                -1,
                -1,
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

        // Should get a query log indicating invalid protocol version was used.
        // Since the error message is sent first, we sleep a little bit to allow activity log to be
        // populated.
        // This won't be an issue in practice.
        Thread.sleep(50);
        assertThat(boundNode.getLogs().getQueryLogs()).hasSize(1);
        QueryLog log = boundNode.getLogs().getQueryLogs().get(0);
        Frame frame = log.getFrame();
        assertThat(frame.protocolVersion).isEqualTo(6);
        assertThat(frame.warnings).hasSize(1);
        assertThat(frame.warnings.get(0))
            .isEqualTo(
                "This message contains a non-supported protocol version by this node.  STARTUP is inferred, but may not reflect the actual message sent.");
        assertThat(frame.message).isInstanceOf(Startup.class);

        // Try again with protocol version 4, which is supported, this should work on the same
        // connection since the previous message was simply discarded.
        client.write(new Startup());

        // Expect a Ready response.
        response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);
      }
    }
  }

  @Test
  public void testShouldReturnProtocolErrorWhenUsingUnsupportedProtocolVersionV4()
      throws Exception {
    // If connecting with a newer protocol version than the node supports, a protocol error should
    // be sent back.
    NodeSpec node =
        NodeSpec.builder().withPeerInfo("protocol_versions", Lists.newArrayList(3)).build();
    try (BoundNode boundNode = localServer.register(node)) {

      FrameCodec<ByteBuf> frameCodec =
          new FrameCodec<>(
              new ByteBufCodec(),
              Compressor.none(),
              new ProtocolV3ClientCodecs(),
              new ProtocolV4ClientCodecs());

      try (MockClient client = new MockClient(eventLoop, frameCodec)) {
        client.connect(boundNode.getAddress());
        // Write a startup message with protocol version 4 (which is not supported by this node)
        client.write(
            new Frame(
                4,
                false,
                0,
                false,
                null,
                -1,
                -1,
                FrameUtils.emptyCustomPayload,
                Collections.emptyList(),
                new Startup()));

        // Expect a protocol error indicating invalid protocol version.
        Frame response = client.next();
        assertThat(response.message).isInstanceOf(Error.class);
        assertThat(response.protocolVersion).isEqualTo(3);
        Error err = (Error) response.message;
        assertThat(err.code).isEqualTo(ProtocolConstants.ErrorCode.PROTOCOL_ERROR);
        assertThat(err.message).isEqualTo("Invalid or unsupported protocol version");

        // Try again with protocol version 3, which is supported, this should work on the same
        // connection
        // since the previous message was simply discarded.
        client.write(
            new Frame(
                3,
                false,
                0,
                false,
                null,
                -1,
                -1,
                FrameUtils.emptyCustomPayload,
                Collections.emptyList(),
                new Startup()));

        // Expect a Ready response.
        response = client.next();
        assertThat(response.message).isInstanceOf(Ready.class);
      }
    }
  }

  @Test
  public void testClusterActiveConnections() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().withNodes(5).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      // Create clients and ensure active connections on each node, data center, and cluster
      List<MockClient> clients = new ArrayList<>();
      for (int i = 0; i < 5; ++i) {
        BoundNode node = boundCluster.node(i);
        BoundDataCenter dc = node.getDataCenter();
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
        BoundNode node = boundCluster.node(i);
        BoundDataCenter dc = node.getDataCenter();

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
    ClusterSpec cluster = ClusterSpec.builder().withNodes(1, 3, 5).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      List<BoundNode> nodes = new ArrayList<>(boundCluster.getNodes());

      // Create clients and ensure active connections on each node, data center, and cluster
      List<MockClient> clients = new ArrayList<>();
      for (int i = 0; i < nodes.size(); ++i) {
        BoundNode node = nodes.get(i);
        BoundDataCenter dc = node.getDataCenter();

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
        BoundNode node = nodes.get(i);
        BoundDataCenter dc = node.getDataCenter();

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
    try (BoundCluster cluster = localServer.register(ClusterSpec.builder().withNodes(3))) {
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
    try (BoundNode node = localServer.register(NodeSpec.builder())) {
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

  @Test
  public void testTryWithResourcesShouldCloseAllResources() throws Exception {
    EventLoopGroup eventLoop;
    Timer timer;

    try (Server server = Server.builder().build()) {
      // Do nothing here, since this is a unit test, we don't want to create any inet sockets
      // which is what Server does by default.
      eventLoop = server.eventLoopGroup;
      timer = server.timer;
    }

    // event loop should have been closed since a custom one was not provided.
    assertThat(eventLoop.isShutdown()).isTrue();
    // timer should have since a custom one was not provided.
    try {
      timer.newTimeout(
          timeout -> {
            // noop
          },
          1,
          TimeUnit.SECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
  }

  @Test
  public void testTryWithResourcesShouldCloseAllClustersButNotEventLoopIfProvided()
      throws Exception {
    EventLoopGroup eventLoop = new DefaultEventLoopGroup();
    BoundCluster cluster;
    MockClient client;

    try (Server server =
        Server.builder()
            .withAddressResolver(localAddressResolver)
            .withEventLoopGroup(eventLoop, LocalServerChannel.class)
            .build()) {

      cluster = server.register(ClusterSpec.builder().withNodes(5));
      BoundNode node = cluster.node(0);
      SocketAddress address = node.getAddress();
      client = new MockClient(eventLoop);
      client.connect(address);
    }

    // event loop should not have been closed.
    assertThat(eventLoop.isShutdown()).isFalse();
    // timer should have since a custom one was not provided.
    try {
      cluster
          .getServer()
          .timer
          .newTimeout(
              timeout -> {
                // noop
              },
              1,
              TimeUnit.SECONDS);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }
    eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
  }

  @Test
  public void testTryWithResourcesShouldCloseAllClustersButNotEventLoopAndTimerIfProvided()
      throws Exception {
    EventLoopGroup eventLoop = new DefaultEventLoopGroup();
    Timer timer = new HashedWheelTimer();
    BoundCluster cluster;
    MockClient client;

    try (Server server =
        Server.builder()
            .withAddressResolver(localAddressResolver)
            .withTimer(timer)
            .withEventLoopGroup(eventLoop, LocalServerChannel.class)
            .build()) {

      cluster = server.register(ClusterSpec.builder().withNodes(5));
      BoundNode node = cluster.node(0);
      SocketAddress address = node.getAddress();
      client = new MockClient(eventLoop);
      client.connect(address);
    }

    // event loop should not have been closed.
    assertThat(eventLoop.isShutdown()).isFalse();
    // timer should not have since a custom one was not provided.
    cluster
        .getServer()
        .timer
        .newTimeout(
            timeout -> {
              // noop
            },
            1,
            TimeUnit.SECONDS);

    eventLoop.shutdownGracefully(0, 0, TimeUnit.SECONDS);
    timer.stop();
  }

  @Test
  public void testTryWithResourcesShouldCloseAllClustersButNotTimerIfProvided() throws Exception {
    EventLoopGroup eventLoop;
    Timer timer = new HashedWheelTimer();

    try (Server server = Server.builder().withTimer(timer).build()) {
      // Do nothing here, since this is a unit test, we don't want to create any inet sockets
      // which is what Server does by default.
      eventLoop = server.eventLoopGroup;
    }

    // event loop should have been closed since a custom one was not provided.
    assertThat(eventLoop.isShutdown()).isTrue();
    // timer should not have been closed since a custom one was provided.
    timer.newTimeout(
        timeout -> {
          // noop
        },
        1,
        TimeUnit.SECONDS);
    timer.stop();
  }

  @Test
  public void testShouldThrowIllegalStateExceptionWhenUsingClosedServer() throws Exception {
    Server server =
        Server.builder()
            .withAddressResolver(localAddressResolver)
            .withEventLoopGroup(eventLoop, LocalServerChannel.class)
            .build();

    BoundCluster cluster = server.register(ClusterSpec.builder().withNodes(1));
    BoundNode node = cluster.node(0);

    server.close();

    try {
      server.register(ClusterSpec.builder().withNodes(1));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      server.unregister(cluster);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      server.register(NodeSpec.builder().build());
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      server.unregister(node);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    try {
      server.unregisterAll();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException ise) {
      // expected
    }

    // closing should not throw exception if already closed.
    server.close();
  }

  @Test
  public void testRegisterClusterWithSplitTokenAssigner() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

      // Should be 2 DCs.
      assertThat(boundCluster.getDataCenters()).hasSize(1);

      Optional<BoundDataCenter> dc1 = boundCluster.getDataCenters().stream().findFirst();
      assertThat(dc1.isPresent()).isTrue();
      Iterator<BoundNode> iterator = dc1.get().getNodes().iterator();
      BoundNode node1 = iterator.next();
      BoundNode node2 = iterator.next();
      long token1 = Long.parseLong((String) node1.getPeerInfo().get("tokens"));
      long token2 = Long.parseLong((String) node2.getPeerInfo().get("tokens"));
      assertThat(token1).isEqualTo(Long.MIN_VALUE);
      assertThat(token2).isZero();
    }
  }

  @Test
  public void testRegisterClusterWithRandomTokenGenerator() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2).withNumberOfTokens(3).build();
    try (BoundCluster boundCluster = localServer.register(cluster)) {
      // Cluster should be registered.
      assertThat(localServer.getCluster(boundCluster.getId())).isSameAs(boundCluster);

      // Should be 2 DCs.
      assertThat(boundCluster.getDataCenters()).hasSize(1);

      Optional<BoundDataCenter> dc1 = boundCluster.getDataCenters().stream().findFirst();
      assertThat(dc1.isPresent()).isTrue();
      Iterator<BoundNode> iterator = dc1.get().getNodes().iterator();
      BoundNode node1 = iterator.next();
      BoundNode node2 = iterator.next();
      assertThat(((String) node1.getPeerInfo().get("tokens")).split(",").length).isEqualTo(3);
      assertThat(((String) node2.getPeerInfo().get("tokens")).split(",").length).isEqualTo(3);
    }
  }

  @Test
  public void testRegisteringCustomStubMappingAlongsideDefaults() throws Exception {
    String defaultQuery = "SELECT * FROM system_schema.keyspaces";
    String customQuery = "SELECT * FROM test";
    EmptyReturnMetadataHandler customStubMapping = new EmptyReturnMetadataHandler(customQuery);

    Server server =
        Server.builder()
            .withStubMapping(customStubMapping)
            .withEventLoopGroup(eventLoop, LocalServerChannel.class)
            .withAddressResolver(localAddressResolver)
            .build();

    NodeSpec node = NodeSpec.builder().build();
    try (BoundNode boundNode = server.register(node)) {

      try (MockClient client = new MockClient(eventLoop)) {
        client.connect(boundNode.getAddress());

        client.write(new Query(customQuery));
        Frame customMappingResponse = client.next();
        assertThat(customMappingResponse.message).isInstanceOf(Rows.class);

        client.write(new Query(defaultQuery));
        Frame defaultMappingResponse = client.next();
        assertThat(defaultMappingResponse.message).isInstanceOf(Rows.class);
      }
    }
  }
}
