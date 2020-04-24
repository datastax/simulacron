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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.*;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.Action;
import com.datastax.oss.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Test;

public class BoundNodeTest {

  private final BoundCluster cluster = new BoundCluster(ClusterSpec.builder().build(), 0L, null);
  private final BoundDataCenter dc = new BoundDataCenter(cluster);
  private final Timer timer = new HashedWheelTimer();
  private final BoundNode node =
      new BoundNode(
          new LocalAddress(UUID.randomUUID().toString()),
          NodeSpec.builder().withName("node0").withId(0L).withCassandraVersion("3.0.14").build(),
          Collections.emptyMap(),
          cluster,
          dc,
          null,
          timer,
          null, // channel reference only needed for closing, not useful in context of this test.
          false);

  private final BoundNode loggedNode =
      new BoundNode(
          new LocalAddress(UUID.randomUUID().toString()),
          NodeSpec.builder().withName("node0").withId(0L).withCassandraVersion("3.0.14").build(),
          Collections.emptyMap(),
          cluster,
          dc,
          null,
          timer,
          null, // channel reference only needed for closing, not useful in context of this test.
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
  public void shouldHandleUseKeyspaceQuoted() {
    channel.writeInbound(FrameUtils.wrapRequest(new Query("use \"myKs2\"")));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isInstanceOf(SetKeyspace.class);
    SetKeyspace setKs = (SetKeyspace) frame.message;
    // should unquote in response as this is what client expects.
    assertThat(setKs.keyspace).isEqualTo("myKs2");
  }

  @Test
  public void shouldRespondWithVoidWhenNoQueryMatch() {
    channel.writeInbound(FrameUtils.wrapRequest(new Query("select * from foo")));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isSameAs(Void.INSTANCE);
  }

  private QueryOptions options =
      new QueryOptions(
          1, Collections.emptyList(), Collections.emptyMap(), true, 0, null, 8, 0L, null);

  @Test
  public void shouldRespondWithUnpreparedToExecute() {
    byte[] queryId = new byte[] {0x8, 0x6, 0x7, 0x5};
    channel.writeInbound(FrameUtils.wrapRequest(new Execute(queryId, options)));
    Frame frame = channel.readOutbound();

    assertThat(frame.message).isInstanceOf(Unprepared.class);

    Unprepared unprepared = (Unprepared) frame.message;
    assertThat(unprepared.id).isEqualTo(queryId);
  }

  @Test
  public void shouldPrepareAndCreateInternalPrime() {
    String query = "select * from unprimed";
    Prepare prepare = new Prepare(query);
    loggedChannel.writeInbound(FrameUtils.wrapRequest(prepare));
    Frame frame = loggedChannel.readOutbound();

    // Should get a prepared response back.
    assertThat(frame.message).isInstanceOf(Prepared.class);

    Prepared prepared = (Prepared) frame.message;

    // Execute should succeed since bound node creates an internal prime.
    Execute execute = new Execute(prepared.preparedQueryId, options);
    loggedChannel.writeInbound(FrameUtils.wrapRequest(execute));
    frame = loggedChannel.readOutbound();

    // Should get a no rows response back.
    assertThat(frame.message).isInstanceOf(Rows.class);

    // Should be recorded in activity log
    try {
      assertThat(
              loggedNode
                  .getLogs()
                  .getQueryLogs()
                  .stream()
                  .filter(ql -> ql.getQuery().equals(query))
                  .findFirst())
          .isPresent();
    } finally {
      loggedNode.clearLogs();
    }
  }

  @Test
  public void shouldPrepareAndCreateInternalPrimeWithProtocolV5() {
    Set<Integer> supportedVersions = new TreeSet<>();
    supportedVersions.add(5);
    FrameCodec codec = new FrameCodecWrapper(supportedVersions, new ProtocolV5ServerCodecs());

    String query = "select * from unprimed";
    Prepare prepare = new Prepare(query);
    loggedChannel.writeInbound(new Frame(
            5,
            true,
            0,
            false,
            null,
            -1,
            -1,
            Collections.emptyMap(),
            Collections.emptyList(),
            prepare));
    Frame frame = loggedChannel.readOutbound();

    // Should get a prepared response back.
    assertThat(frame.message).isInstanceOf(Prepared.class);

    Prepared prepared = (Prepared) frame.message;

    assertThat(codec.encode(frame)).isNotNull();

    // Execute should succeed since bound node creates an internal prime.
    Execute execute = new Execute(prepared.preparedQueryId, options);
    loggedChannel.writeInbound(new Frame(
            5,
            true,
            0,
            false,
            null,
            -1,
            -1,
            Collections.emptyMap(),
            Collections.emptyList(),
            execute));
    frame = loggedChannel.readOutbound();

    // Should get a no rows response back.
    assertThat(frame.message).isInstanceOf(Rows.class);

    // Should be recorded in activity log
    try {
      assertThat(
              loggedNode
                      .getLogs()
                      .getQueryLogs()
                      .stream()
                      .filter(ql -> ql.getQuery().equals(query))
                      .findFirst())
              .isPresent();
    } finally {
      loggedNode.clearLogs();
    }

    assertThat(codec.encode(frame)).isNotNull();
  }

  @Test
  public void shouldRespondWithStubActionsWhenMatched() {
    String query = "select * from foo where bar = ?";
    RowsMetadata rowsMetadata = new RowsMetadata(Collections.emptyList(), null, new int[0], null);
    Prepared response = new Prepared(new byte[] {1, 7, 9}, null, rowsMetadata, rowsMetadata);
    node.getStubStore()
        .register(
            new StubMapping() {

              @Override
              public boolean matches(Frame frame) {
                Message msg = frame.message;
                if (msg instanceof Prepare) {
                  Prepare p = (Prepare) msg;
                  return p.cqlQuery.equals(query);
                }
                return false;
              }

              @Override
              public List<Action> getActions(AbstractNode node, Frame frame) {
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
    RowsMetadata rowsMetadata = new RowsMetadata(Collections.emptyList(), null, new int[0], null);
    Prepared response = new Prepared(new byte[] {1, 7, 9}, null, rowsMetadata, rowsMetadata);
    node.getStubStore()
        .register(
            new StubMapping() {

              @Override
              public boolean matches(Frame frame) {
                Message msg = frame.message;
                if (msg instanceof Prepare) {
                  Prepare p = (Prepare) msg;
                  return p.cqlQuery.equals(query);
                }
                return false;
              }

              @Override
              public List<Action> getActions(AbstractNode node, Frame frame) {
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
            Long.MIN_VALUE,
            null);
    Query query2 = new Query("select * from table1", options);
    Frame request2 = FrameUtils.wrapRequest(query2);
    loggedChannel.writeInbound(request1);
    loggedChannel.readOutbound();
    loggedChannel.writeInbound(request2);
    loggedChannel.readOutbound();

    List<QueryLog> logs = loggedNode.getLogs().getQueryLogs();
    assertThat(logs.size()).isEqualTo(2);
    QueryLog log1 = logs.get(0);
    assertThat(log1.getQuery()).isEqualTo("use myks");
    assertThat(log1.getConsistency()).isEqualTo(ConsistencyLevel.ONE);
    QueryLog log2 = logs.get(1);
    assertThat(log2.getQuery()).isEqualTo("select * from table1");
    assertThat(log2.getConsistency()).isEqualTo(ConsistencyLevel.QUORUM);
  }

  @Test
  public void shouldNotifyNodeListener() {
    CompletableFuture<QueryLog> logF = new CompletableFuture<>();
    CompletableFuture<BoundNode> nodeF = new CompletableFuture<>();
    loggedNode.registerQueryListener(
        (n, l) -> {
          nodeF.complete(n);
          logF.complete(l);
        });
    Query query = new Query("select * from table1");
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(CompletableFutures.getUninterruptibly(logF, 5, TimeUnit.SECONDS).getQuery())
        .isEqualTo(query.query);
    assertThat(CompletableFutures.getUninterruptibly(nodeF, 5, TimeUnit.SECONDS))
        .isEqualTo(loggedNode);
  }

  @Test
  public void shouldNotifyListenerPredicateMatch() {
    CompletableFuture<QueryLog> logF = new CompletableFuture<>();
    CompletableFuture<BoundNode> nodeF = new CompletableFuture<>();
    Query query = new Query("select * from table1");
    loggedNode.registerQueryListener(
        (n, l) -> {
          nodeF.complete(n);
          logF.complete(l);
        },
        false,
        (l) -> l.getQuery().equals(query.query));
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(CompletableFutures.getUninterruptibly(logF, 5, TimeUnit.SECONDS).getQuery())
        .isEqualTo(query.query);
    assertThat(CompletableFutures.getUninterruptibly(nodeF, 5, TimeUnit.SECONDS))
        .isEqualTo(loggedNode);
  }

  @Test
  public void shouldNotNotifyListenerPredicateNotMatch() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    Query query = new Query("select * from table1");
    loggedNode.registerQueryListener(
        (n, l) -> latch.countDown(), false, (l) -> !l.getQuery().equals(query.query));
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(latch.await(100, TimeUnit.MILLISECONDS)).isFalse();
  }

  @Test
  public void shouldNotifyDCListener() {
    CompletableFuture<QueryLog> logF = new CompletableFuture<>();
    CompletableFuture<BoundNode> nodeF = new CompletableFuture<>();
    dc.registerQueryListener(
        (n, l) -> {
          nodeF.complete(n);
          logF.complete(l);
        });
    Query query = new Query("select * from table1");
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(CompletableFutures.getUninterruptibly(logF, 5, TimeUnit.SECONDS).getQuery())
        .isEqualTo(query.query);
    assertThat(CompletableFutures.getUninterruptibly(nodeF, 5, TimeUnit.SECONDS))
        .isEqualTo(loggedNode);
  }

  @Test
  public void shouldNotifyClusterListener() {
    CompletableFuture<QueryLog> logF = new CompletableFuture<>();
    CompletableFuture<BoundNode> nodeF = new CompletableFuture<>();
    cluster.registerQueryListener(
        (n, l) -> {
          nodeF.complete(n);
          logF.complete(l);
        });
    Query query = new Query("select * from table1");
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(CompletableFutures.getUninterruptibly(logF, 5, TimeUnit.SECONDS).getQuery())
        .isEqualTo(query.query);
    assertThat(CompletableFutures.getUninterruptibly(nodeF, 5, TimeUnit.SECONDS))
        .isEqualTo(loggedNode);
  }

  @Test
  public void shouldNotifyNodeListenerAfter() {
    CompletableFuture<QueryLog> logF = new CompletableFuture<>();
    CompletableFuture<BoundNode> nodeF = new CompletableFuture<>();
    loggedNode.registerQueryListener(
        (n, l) -> {
          nodeF.complete(n);
          logF.complete(l);
        },
        true);
    Query query = new Query("select * from table1");
    Frame request = FrameUtils.wrapRequest(query);

    loggedChannel.writeInbound(request);
    loggedChannel.readInbound();

    assertThat(CompletableFutures.getUninterruptibly(logF, 5, TimeUnit.SECONDS).getQuery())
        .isEqualTo(query.query);
    assertThat(CompletableFutures.getUninterruptibly(nodeF, 5, TimeUnit.SECONDS))
        .isEqualTo(loggedNode);
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

    assertThat(node.getLogs().getQueryLogs()).hasSize(0);
  }
}
