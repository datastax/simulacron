package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.CloseConnectionAction;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.simulacron.common.stubbing.StubMapping;
import io.netty.channel.ChannelFuture;
import io.netty.channel.DefaultEventLoop;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.local.LocalServerChannel;
import org.junit.After;
import org.junit.Test;

import java.nio.channels.ClosedChannelException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.datastax.simulacron.server.AddressResolver.localAddressResolver;
import static org.assertj.core.api.Assertions.assertThat;

public class CloseActionTest {

  private final EventLoopGroup eventLoop = new DefaultEventLoop();

  private final Server localServer =
      Server.builder(eventLoop, LocalServerChannel.class)
          .withAddressResolver(localAddressResolver)
          .build();

  @After
  public void tearDown() {
    eventLoop.shutdownGracefully();
  }

  @Test
  public void testClose() throws Exception {
    // Validate that connection closes when a stubbed action tells it to.
    Node node = Node.builder().build();
    Node boundNode = localServer.register(node).get(5, TimeUnit.SECONDS);

    localServer
        .getStubStore()
        .register(
            new StubMapping() {
              @Override
              public boolean matches(Node node, Frame frame) {
                return frame.message instanceof Startup;
              }

              @Override
              public List<Action> getActions(Node node, Frame frame) {
                return Collections.singletonList(new CloseConnectionAction());
              }
            });

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
      }
    }
  }

  @Test
  public void testMessageWithClose() throws Exception {
    // Validate that connection closes a stub dictates to send a message and then close.
    Node node = Node.builder().build();
    Node boundNode = localServer.register(node).get(5, TimeUnit.SECONDS);

    localServer
        .getStubStore()
        .register(
            new StubMapping() {
              @Override
              public boolean matches(Node node, Frame frame) {
                return frame.message instanceof Options;
              }

              @Override
              public List<Action> getActions(Node node, Frame frame) {
                ArrayList<Action> actions = new ArrayList<>();
                actions.add(new MessageResponseAction(new Supported(Collections.emptyMap())));
                actions.add(new CloseConnectionAction());
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
}
