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

import static com.datastax.oss.simulacron.server.AddressResolver.localAddressResolver;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.Action;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction.Scope;
import com.datastax.oss.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class DisconnectActionTest {

  private final EventLoopGroup eventLoop = new DefaultEventLoop();

  private final Server localServer =
      Server.builder()
          .withEventLoopGroup(eventLoop, LocalServerChannel.class)
          .withAddressResolver(localAddressResolver)
          .build();

  @After
  public void tearDown() {
    eventLoop.shutdownGracefully();
  }

  @Test
  public void testCloseConnection() throws Exception {
    // Validate that when a stub dictates to close a connection it does so and does not close the NodeSpec's channel so it
    // can remain accepting traffic.
    NodeSpec node = NodeSpec.builder().build();
    BoundNode boundNode = localServer.register(node);

    stubCloseOnStartup(Scope.CONNECTION);

    try (MockClient client = new MockClient(eventLoop)) {
      client.connect(boundNode.getAddress());
      // Sending a write should cause the connection to close.
      ChannelFuture f = client.write(new Startup());
      // Future should be successful since write was successful.
      f.get(5, TimeUnit.SECONDS);
      // Next write should fail because the channel was closed.
      f = client.write(Options.INSTANCE);
      try {
        f.get();
      } catch (ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
      } finally {
        assertThat(client.channel.isOpen()).isFalse();
        // node should still accept connections.
        assertThat(boundNode.channel.get().isOpen()).isTrue();
      }
    }
  }

  @Test
  public void testMessageWithClose() throws Exception {
    // Validates that a stub that dictates to send a message and then close a connection does so.
    NodeSpec node = NodeSpec.builder().build();
    BoundNode boundNode = localServer.register(node);

    localServer.stubStore.register(
        new StubMapping() {
          @Override
          public boolean matches(Frame frame) {
            return frame.message instanceof Options;
          }

          @Override
          public List<Action> getActions(AbstractNode node, Frame frame) {
            ArrayList<Action> actions = new ArrayList<>();
            actions.add(new MessageResponseAction(new Supported(Collections.emptyMap())));
            actions.add(DisconnectAction.builder().build());
            return actions;
          }
        });

    try (MockClient client = new MockClient(eventLoop)) {
      client.connect(boundNode.getAddress());
      // Sending a write should cause the connection to send a supported response and then close.
      ChannelFuture f = client.write(Options.INSTANCE);
      // Future should be successful since write was successful.
      f.get(5, TimeUnit.SECONDS);
      assertThat(client.next().message).isInstanceOf(Supported.class);
      // Next write should fail because the channel was closed.
      f = client.write(new Startup());
      try {
        f.get();
      } catch (ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
      } finally {
        assertThat(client.channel.isOpen()).isFalse();
      }
    }
  }

  @Test
  public void testCloseNode() throws Exception {
    // Validates that a stub that dictates to close a node's connections does so.
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2, 2).build();
    BoundCluster boundCluster = localServer.register(cluster);

    BoundDataCenter dc0 = boundCluster.getDataCenters().iterator().next();
    Iterator<BoundNode> nodes = dc0.getNodes().iterator();
    BoundNode boundNode = nodes.next();
    stubCloseOnStartup(Scope.NODE);

    Map<BoundNode, MockClient> nodeToClients = new HashMap<>();
    MockClient client = null;
    try {
      // Create a connection to each node.
      for (BoundNode node : boundCluster.getNodes()) {
        MockClient client0 = new MockClient(eventLoop);
        client0.connect(node.getAddress());
        nodeToClients.put(node, client0);
      }

      client = new MockClient(eventLoop);
      client.connect(boundNode.getAddress());

      // Sending a write should cause the connection to close.
      ChannelFuture f = client.write(new Startup());
      // Future should be successful since write was successful.
      f.get(5, TimeUnit.SECONDS);
      // Next write should fail because the channel was closed.
      f = client.write(Options.INSTANCE);
      try {
        f.get();
      } catch (ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
      }
    } finally {
      if (client != null) {
        // client that sent request should close.
        assertThat(client.channel.isOpen()).isFalse();
      }
      // All clients should remain open except the ones to the node that received the request.
      nodeToClients
          .entrySet()
          .stream()
          .filter(e -> e.getKey() != boundNode)
          .forEach(e -> assertThat(e.getValue().channel.isOpen()).isTrue());
      nodeToClients
          .entrySet()
          .stream()
          .filter(e -> e.getKey() == boundNode)
          .forEach(e -> assertThat(e.getValue().channel.isOpen()).isFalse());
    }
  }

  @Test
  public void testCloseDataCenter() throws Exception {
    // Validates that a stub that dictates to close a node's DC's connections does so.
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2, 2).build();
    BoundCluster boundCluster = localServer.register(cluster);

    BoundDataCenter dc0 = boundCluster.getDataCenters().iterator().next();
    Iterator<BoundNode> nodes = dc0.getNodes().iterator();
    BoundNode boundNode = nodes.next();
    stubCloseOnStartup(Scope.DATA_CENTER);

    Map<BoundNode, MockClient> nodeToClients = new HashMap<>();
    MockClient client = null;
    try {
      // Create a connection to each node.
      for (BoundNode node : boundCluster.getNodes()) {
        MockClient client0 = new MockClient(eventLoop);
        client0.connect(node.getAddress());
        nodeToClients.put(node, client0);
      }

      client = new MockClient(eventLoop);
      client.connect(boundNode.getAddress());

      // Sending a write should cause the connection to close.
      ChannelFuture f = client.write(new Startup());
      // Future should be successful since write was successful.
      f.get(5, TimeUnit.SECONDS);
      // Next write should fail because the channel was closed.
      f = client.write(Options.INSTANCE);
      try {
        f.get();
      } catch (ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
      }
    } finally {
      if (client != null) {
        // client that sent request should close.
        assertThat(client.channel.isOpen()).isFalse();
      }
      // Clients connecting to a different DC should remain open.
      nodeToClients
          .entrySet()
          .stream()
          .filter(e -> e.getKey().getDataCenter() != boundNode.getDataCenter())
          .forEach(e -> assertThat(e.getValue().channel.isOpen()).isTrue());
      // Clients connecting to same DC should close.
      nodeToClients
          .entrySet()
          .stream()
          .filter(e -> e.getKey().getDataCenter() == boundNode.getDataCenter())
          .forEach(e -> assertThat(e.getValue().channel.isOpen()).isFalse());
    }
  }

  @Test
  public void testCloseCluster() throws Exception {
    // Validates that a stub that dictates to close a node's ClusterSpec's connections does so.
    ClusterSpec cluster = ClusterSpec.builder().withNodes(2, 2).build();
    BoundCluster boundCluster = localServer.register(cluster);

    BoundDataCenter dc0 = boundCluster.getDataCenters().iterator().next();
    Iterator<BoundNode> nodes = dc0.getNodes().iterator();
    BoundNode boundNode = nodes.next();
    stubCloseOnStartup(Scope.CLUSTER);

    Map<BoundNode, MockClient> nodeToClients = new HashMap<>();
    MockClient client = null;
    try {
      // Create a connection to each node.
      for (BoundNode node : boundCluster.getNodes()) {
        MockClient client0 = new MockClient(eventLoop);
        client0.connect(node.getAddress());
        nodeToClients.put(node, client0);
      }

      client = new MockClient(eventLoop);
      client.connect(boundNode.getAddress());

      // Sending a write should cause the connection to close.
      ChannelFuture f = client.write(new Startup());
      // Future should be successful since write was successful.
      f.get(5, TimeUnit.SECONDS);
      // Next write should fail because the channel was closed.
      f = client.write(Options.INSTANCE);
      try {
        f.get();
      } catch (ExecutionException e) {
        assertThat(e.getCause()).isInstanceOf(ClosedChannelException.class);
      }
    } finally {
      if (client != null) {
        // client that sent request should close.
        assertThat(client.channel.isOpen()).isFalse();
      }
      // All clients should close
      nodeToClients.entrySet().forEach(e -> assertThat(e.getValue().channel.isOpen()).isFalse());
    }
  }

  private void stubCloseOnStartup(Scope scope) {
    localServer.stubStore.register(
        new StubMapping() {
          @Override
          public boolean matches(Frame frame) {
            return frame.message instanceof Startup;
          }

          @Override
          public List<Action> getActions(AbstractNode node, Frame frame) {
            return Collections.singletonList(DisconnectAction.builder().withScope(scope).build());
          }
        });
  }
}
