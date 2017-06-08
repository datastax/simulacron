package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.simulacron.common.stubbing.StubMapping;
import com.datastax.simulacron.common.utils.FrameUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class BoundNodeTest {

  private final Cluster cluster = Cluster.builder().build();
  private final DataCenter dc = cluster.addDataCenter().build();
  private final StubStore stubStore = new StubStore();
  private final Timer timer = new HashedWheelTimer();
  private final BoundNode node =
      new BoundNode(
          new LocalAddress(UUID.randomUUID().toString()),
          "node0",
          0L,
          "1.2.19",
          null,
          Collections.emptyMap(),
          dc,
          null,
          timer,
          null, // channel reference only needed for closing, not useful in context of this test.
          stubStore,
          false);

  private final BoundNode loggedNode =
      new BoundNode(
          new LocalAddress(UUID.randomUUID().toString()),
          "node0",
          0L,
          "1.2.19",
          null,
          Collections.emptyMap(),
          dc,
          null,
          timer,
          null, // channel reference only needed for closing, not useful in context of this test.
          stubStore,
          true);

  private final EmbeddedChannel channel = new EmbeddedChannel(new RequestHandler(node));
  private final EmbeddedChannel loggedChannel = new EmbeddedChannel(new RequestHandler(loggedNode));

  @After
  public void tearDown() {
    timer.stop();
  }

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
  public void shouldRespondWithStubActionsWhenMatched() throws Exception {
    String query = "select * from foo where bar = ?";
    RowsMetadata rowsMetadata = new RowsMetadata(Collections.emptyList(), null, new int[0]);
    Prepared response = new Prepared(new byte[] {1, 7, 9}, rowsMetadata, rowsMetadata);
    stubStore.register(
        new StubMapping() {

          @Override
          public boolean matches(Frame frame) {
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

  @Test
  public void shouldRespondWithDelay() throws Exception {
    String query = "select * from foo where bar = ?";
    RowsMetadata rowsMetadata = new RowsMetadata(Collections.emptyList(), null, new int[0]);
    Prepared response = new Prepared(new byte[] {1, 7, 9}, rowsMetadata, rowsMetadata);
    stubStore.register(
        new StubMapping() {

          @Override
          public boolean matches(Frame frame) {
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
            List<Action> actions = new ArrayList<>();
            actions.add(new MessageResponseAction(response, 500));
            actions.add(new MessageResponseAction(Options.INSTANCE));
            return actions;
          }
        });

    channel.writeInbound(FrameUtils.wrapRequest(new Prepare(query)));

    // Should be no message immediately due to delay used.
    Frame frame = channel.readOutbound();
    assertThat(frame).isNull();

    // Wait a second for action to be processed
    TimeUnit.SECONDS.sleep(1);

    frame = channel.readOutbound();
    assertThat(frame.message).isSameAs(response);

    // Should be another action that is processed immediately after that is an options message.
    frame = channel.readOutbound();
    assertThat(frame.message).isSameAs(Options.INSTANCE);
  }

  @Test
  public void shouldStoreActivityLogIfEnabled() {
    Frame request1 = FrameUtils.wrapRequest(new Query("use myks"));
    QueryOptions options =
        new QueryOptions(
            ProtocolConstants.ConsistencyLevel.QUORUM,
            Collections.emptyList(),
            Collections.emptyMap(),
            false,
            -1,
            null,
            ProtocolConstants.ConsistencyLevel.SERIAL,
            Long.MIN_VALUE);
    Query query2 = new Query("select * from table1", options);
    Frame request2 = FrameUtils.wrapRequest(query2);
    loggedChannel.writeInbound(request1);
    loggedChannel.readOutbound();
    loggedChannel.writeInbound(request2);
    loggedChannel.readOutbound();

    ActivityLog activityLog = loggedNode.getCluster().getActivityLog();
    assertThat(activityLog.getSize()).isEqualTo(2);
    QueryLog log1 = activityLog.getLogs().get(0);
    assertThat(log1.getQuery()).isEqualTo("use myks");
    assertThat(ConsistencyLevel.fromString(log1.getConsistency())).isEqualTo(ConsistencyLevel.ONE);
    QueryLog log2 = activityLog.getLogs().get(1);
    assertThat(log2.getQuery()).isEqualTo("select * from table1");
    assertThat(ConsistencyLevel.fromString(log2.getConsistency()))
        .isEqualTo(ConsistencyLevel.QUORUM);
  }

  @Test
  public void shouldNotStoreActivityLogIfDisabled() {
    Frame request1 = FrameUtils.wrapRequest(new Query("use myks"));
    Query query2 = new Query("select * from table1");
    Frame request2 = FrameUtils.wrapRequest(query2);
    channel.writeInbound(request1);
    channel.readOutbound();
    channel.writeInbound(request2);
    channel.readOutbound();

    ActivityLog activityLog = node.getCluster().getActivityLog();
    assertThat(activityLog.getSize()).isEqualTo(0);
  }
}
