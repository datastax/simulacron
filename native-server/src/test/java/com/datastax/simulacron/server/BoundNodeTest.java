package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.simulacron.common.stubbing.StubMapping;
import com.datastax.simulacron.common.utils.FrameUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

public class BoundNodeTest {

  private final Cluster cluster = Cluster.builder().build();
  private final DataCenter dc = cluster.addDataCenter().build();
  private final StubStore stubStore = new StubStore();
  private final BoundNode node =
      new BoundNode(
          new LocalAddress(UUID.randomUUID().toString()),
          "node0",
          0L,
          "1.2.19",
          Collections.emptyMap(),
          dc,
          null, // channel reference only needed for closing, not useful in context of this test.
          stubStore);

  private final EmbeddedChannel channel = new EmbeddedChannel(new RequestHandler(node));

  @Test
  public void shouldHandleStartup() {
    channel.writeInbound(FrameUtils.wrapRequest(new Startup()));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isInstanceOf(Ready.class);
  }

  @Test
  public void shouldHandleOptions() {
    channel.writeInbound(FrameUtils.wrapRequest(Options.INSTANCE));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isInstanceOf(Supported.class);
  }

  @Test
  public void shouldHandleUseKeyspace() {
    channel.writeInbound(FrameUtils.wrapRequest(new Query("use myks")));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isInstanceOf(SetKeyspace.class);
    SetKeyspace setKs = (SetKeyspace) frame.message;
    assertThat(setKs.keyspace).isEqualTo("myks");
  }

  @Test
  public void shouldResponseWithVoidWhenNoQueryMatch() {
    channel.writeInbound(FrameUtils.wrapRequest(new Query("select * from foo")));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isSameAs(Void.INSTANCE);
  }

  @Test
  public void shouldRespondWithStubActionsWhenMatched() {
    String query = "select * from foo where bar = ?";
    RowsMetadata rowsMetadata = new RowsMetadata(Collections.emptyList(), null, new int[0]);
    Prepared response = new Prepared(new byte[] {1, 7, 9}, rowsMetadata, rowsMetadata);
    stubStore.register(
        new StubMapping() {

          @Override
          public boolean matches(Node node, Frame frame) {
            Message msg = frame.message;
            if (msg instanceof Prepare) {
              Prepare p = (Prepare) msg;
              if (p.cqlQuery.equals(query)) {
                return true;
              }
            }
            return false;
          }

          @Override
          public List<Action> getActions(Node node, Frame frame) {
            return Collections.singletonList(new MessageResponseAction(response));
          }
        });

    channel.writeInbound(FrameUtils.wrapRequest(new Prepare(query)));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isSameAs(response);
  }
}
