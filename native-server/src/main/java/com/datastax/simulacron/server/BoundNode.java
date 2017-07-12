package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.*;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.error.Unprepared;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.stubbing.*;
import com.datastax.simulacron.common.stubbing.PrimeDsl.PrimeBuilder;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.util.Timeout;
import io.netty.util.Timer;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.oss.protocol.internal.response.result.Void.INSTANCE;
import static com.datastax.simulacron.common.stubbing.DisconnectAction.Scope.CLUSTER;
import static com.datastax.simulacron.common.stubbing.DisconnectAction.Scope.NODE;
import static com.datastax.simulacron.common.stubbing.PrimeDsl.noRows;
import static com.datastax.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.simulacron.common.utils.FrameUtils.wrapResponse;
import static com.datastax.simulacron.server.ChannelUtils.completable;

public class BoundNode extends Node implements BoundTopic<NodeConnectionReport> {

  private static Logger logger = LoggerFactory.getLogger(BoundNode.class);

  private static final Pattern useKeyspacePattern =
      Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE);

  private final transient ServerBootstrap bootstrap;

  // TODO: Isn't really a good reason for this to be an AtomicReference as if binding fails we don't reset
  // the channel, but leaving it this way for now in case there is a future use case.
  final transient AtomicReference<Channel> channel;

  final transient ChannelGroup clientChannelGroup =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  // TODO: There could be a lot of concurrency issues around simultaneous calls to reject/accept, however
  // in the general case we don't expect it.   Leave this as AtomicReference in case we want to handle it better.
  private final transient AtomicReference<RejectState> rejectState =
      new AtomicReference<>(new RejectState());

  private final transient Timer timer;

  private final transient StubStore stubStore;

  private final boolean activityLogging;

  private final Server server;

  private final BoundCluster cluster;

  private final transient ActivityLog activityLog = new ActivityLog();

  private static class RejectState {
    private final RejectScope scope;
    private volatile int rejectAfter;
    private volatile boolean listeningForNewConnections;

    RejectState() {
      this(true, Integer.MIN_VALUE, null);
    }

    RejectState(boolean listeningForNewConnections, int rejectAfter, RejectScope scope) {
      this.listeningForNewConnections = listeningForNewConnections;
      this.rejectAfter = rejectAfter;
      this.scope = scope;
    }
  }

  BoundNode(
      SocketAddress address,
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      BoundCluster cluster,
      DataCenter parent,
      Server server,
      Timer timer,
      Channel channel,
      boolean activityLogging) {
    super(address, name, id, cassandraVersion, dseVersion, peerInfo, parent);
    this.cluster = cluster;
    this.server = server;
    // for test purposes server may be null.
    this.bootstrap = server != null ? server.serverBootstrap : null;
    this.timer = timer;
    this.channel = new AtomicReference<>(channel);
    this.stubStore = new StubStore();
    this.activityLogging = activityLogging;
  }

  @Override
  public Long getActiveConnections() {
    // Filter only active channels as some may be in process of closing.
    return clientChannelGroup.stream().filter(Channel::isActive).count();
  }

  /**
   * Closes the listening channel for this node. Note that this does not close existing client
   * connections, this can be done using {@link #disconnectConnections()}. To stop listening and
   * close connections, use {@link #close()}.
   *
   * @return future that completes when listening channel is closed.
   */
  private CompletableFuture<Void> unbind() {
    logger.debug("Unbinding listener on {}", channel);
    return completable(channel.get().close()).thenApply(v -> null);
  }

  /**
   * Reopens the listening channel for this node. If the channel was already open, has no effect and
   * future completes immediately.
   *
   * @return future that completes when listening channel is reopened.
   */
  private CompletableFuture<Void> rebind() {
    if (this.channel.get().isOpen()) {
      // already accepting...
      return CompletableFuture.completedFuture(null);
    }
    CompletableFuture<Void> future = new CompletableFuture<>();
    ChannelFuture bindFuture = bootstrap.bind(this.getAddress());
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              if (channelFuture.isSuccess()) {
                channelFuture.channel().attr(Server.HANDLER).set(this);
                logger.debug("Bound {} to {}", BoundNode.this, channelFuture.channel());
                future.complete(null);
                channel.set(channelFuture.channel());
              } else {
                // If failed, propagate it.
                future.completeExceptionally(
                    new BindNodeException(BoundNode.this, getAddress(), channelFuture.cause()));
              }
            });
    return future;
  }

  /**
   * Disconnects all client channels. Does not close listening interface (see {@link #unbind()} for
   * that).
   *
   * @return future that completes when all client channels are disconnected.
   */
  private CompletionStage<Void> disconnectConnections() {
    return completable(clientChannelGroup.disconnect()).thenApply(v -> null);
  }

  /**
   * Indicates that the node should resume accepting connections.
   *
   * @return future that completes when node is listening again.
   */
  @Override
  public CompletionStage<Void> acceptConnectionsAsync() {
    logger.debug("Accepting New Connections");
    rejectState.set(new RejectState());
    // Reopen listening interface if not currently open.
    if (!channel.get().isOpen()) {
      return rebind();
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * This is used to fetch the QueryLogReport with unfiltered activity logs
   *
   * @return QueryLogReport containing all the logs for the node
   */
  @Override
  @JsonIgnore
  public QueryLogReport getLogs() {
    ClusterQueryLogReport clusterQueryLogReportReport = new ClusterQueryLogReport(cluster.getId());
    return clusterQueryLogReportReport.addNode(this, this.activityLog.getLogs());
  }

  /**
   * This is used to fetch the QueryLogReport with filtered activity logs
   *
   * @return QueryLogReport containing all the logs for the node
   */
  @Override
  @JsonIgnore
  public QueryLogReport getLogs(boolean primed) {
    ClusterQueryLogReport clusterQueryLogReportReport = new ClusterQueryLogReport(cluster.getId());
    return clusterQueryLogReportReport.addNode(this, this.activityLog.getLogs(primed));
  }

  /**
   * This is used to fetch all the QueryLogs from the nodes activity logs
   *
   * @return List of QueryLog for the node
   */
  public List<QueryLog> getActivityLogs() {
    return this.activityLog.getLogs();
  }
  /**
   * This is used to fetch filtered QueryLogs from the nodes activity logs
   *
   * @return List of QueryLog for the node
   */
  public List<QueryLog> getActivityLogsWithFiltering(boolean primed) {
    return this.activityLog.getLogs(primed);
  }

  @Override
  public void clearLogs() {
    activityLog.clear();
  }

  /**
   * Indicates that the node should stop accepting new connections.
   *
   * @param after If non-zero, after how many successful startup messages should stop accepting
   *     connections.
   * @param scope The scope to reject connections, either stop listening for connections, or accept
   *     connections but don't respond to startup requests.
   * @return future that completes when listening channel is unbound (if {@link RejectScope#UNBIND}
   *     was used) or immediately if {@link RejectScope#REJECT_STARTUP} was used or after > 0.
   */
  @Override
  public CompletionStage<Void> rejectConnectionsAsync(int after, RejectScope scope) {
    RejectState state;
    if (after <= 0) {
      logger.debug("Rejecting new connections with scope {}", scope);
      state = new RejectState(false, Integer.MIN_VALUE, scope);
    } else {
      logger.debug("Rejecting new connections after {} attempts with scope {}", after, scope);
      state = new RejectState(true, after, scope);
    }
    rejectState.set(state);
    if (after <= 0 && scope != RejectScope.REJECT_STARTUP) {
      CompletableFuture<Void> unbindFuture = unbind();
      // if scope is STOP, disconnect existing connections after unbinding.
      if (scope == RejectScope.STOP) {
        return unbindFuture.thenCompose(n -> disconnectConnections());
      } else {
        return unbindFuture;
      }
    } else {
      return CompletableFuture.completedFuture(null);
    }
  }

  /**
   * Search stub stores for matches for the given frame and this node. If not found at node level,
   * checks dc level, if not found at dc level, checks cluster level, if not found at cluster level,
   * checks global store.
   *
   * @param frame frame to match on.
   * @return matching stub if present.
   */
  private Optional<StubMapping> find(Frame frame) {
    Optional<StubMapping> stub = stubStore.find(this, frame);
    if (!stub.isPresent()) {
      return Optional.ofNullable(getDataCenter()).flatMap(dc -> dc.find(BoundNode.this, frame));
    }
    return stub;
  }

  void handle(ChannelHandlerContext ctx, Frame frame) {
    logger.debug("Got request streamId: {} msg: {}", frame.streamId, frame.message);
    // On receiving a message, first check the stub store to see if there is handling logic for it.
    // If there is, handle each action.
    // Otherwise delegate to default behavior.
    Optional<StubMapping> stubOption = find(frame);
    List<Action> actions = null;
    if (stubOption.isPresent()) {
      StubMapping stub = stubOption.get();
      actions = stub.getActions(this, frame);
    }

    //store the frame in history
    if (activityLogging) {
      activityLog.addLog(
          this, frame, ctx.channel().remoteAddress(), stubOption, System.currentTimeMillis());
    }

    if (actions != null && !actions.isEmpty()) {
      // TODO: It might be useful to tie behavior to completion of actions but for now this isn't necessary.
      CompletableFuture<Void> future = new CompletableFuture<>();
      handleActions(actions.iterator(), ctx, frame, future);
    } else {
      // Future that if set defers sending the message until the future completes.
      CompletableFuture<?> deferFuture = null;
      Message response = null;
      if (frame.message instanceof Startup || frame.message instanceof Register) {
        RejectState state = rejectState.get();
        // We aren't listening for new connections, return immediately.
        if (!state.listeningForNewConnections) {
          return;
        } else if (state.rejectAfter > 0) {
          // Decrement rejectAfter indicating a new initialization attempt.
          state.rejectAfter--;
          if (state.rejectAfter == 0) {
            // If reject after is now 0, indicate that it's time to stop listening (but allow this one)
            state.rejectAfter = -1;
            state.listeningForNewConnections = false;
            deferFuture = rejectConnectionsAsync(-1, state.scope).toCompletableFuture();
          }
        }
        response = new Ready();
      } else if (frame.message instanceof Options) {
        // Maybe eventually we can set these depending on the version but so far it looks
        // like this.cassandraVersion and this.dseVersion are both null
        HashMap<String, List<String>> options = new HashMap<>();
        options.put("PROTOCOL_VERSIONS", Arrays.asList("3/v3", "4/v4", "5/v5-beta"));
        options.put("CQL_VERSION", Collections.singletonList("3.4.4"));
        options.put("COMPRESSION", Arrays.asList("snappy", "lz4"));

        response = new Supported(options);
      } else if (frame.message instanceof Query) {
        Query query = (Query) frame.message;
        String queryStr = query.query;
        if (queryStr.startsWith("USE") || queryStr.startsWith("use")) {
          Matcher matcher = useKeyspacePattern.matcher(queryStr);
          // should always match.
          assert matcher.matches();
          if (matcher.matches()) {
            // unquote keyspace if quoted, cassandra doesn't expect keyspace to be quoted coming back
            String keyspace = matcher.group(1).replaceAll("^\"|\"$", "");
            response = new SetKeyspace(keyspace);
          }
        } else {
          response = INSTANCE;
        }
      } else if (frame.message instanceof Execute) {
        // Unprepared execute received, return an unprepared.
        Execute execute = (Execute) frame.message;
        String hex = new BigInteger(1, execute.queryId).toString(16);
        response = new Unprepared("No prepared statement with id: " + hex, execute.queryId);
      } else if (frame.message instanceof Prepare) {
        // fake up a prepared statement from the message and register an internal prime for it.
        Prepare prepare = (Prepare) frame.message;
        // TODO: Maybe attempt to identify bind parameters
        String query = prepare.cqlQuery;
        Prime prime = whenWithInferredParams(query).then(noRows()).build();
        this.getBoundCluster().getStubStore().registerInternal(prime);
        response = prime.toPrepared();
      }
      if (response != null) {
        if (deferFuture != null) {
          final Message fResponse = response;
          deferFuture.thenRun(() -> sendMessage(ctx, frame, fResponse));
        } else {
          sendMessage(ctx, frame, response);
        }
      }
    }
  }

  private void handleActions(
      Iterator<Action> nextActions,
      ChannelHandlerContext ctx,
      Frame frame,
      CompletableFuture<Void> doneFuture) {
    // If there are no more actions, complete the done future and return.
    if (!nextActions.hasNext()) {
      doneFuture.complete(null);
      return;
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    Action action = nextActions.next();
    ActionHandler handler = new ActionHandler(action, ctx, frame, future);
    if (action.delayInMs() > 0) {
      timer.newTimeout(handler, action.delayInMs(), TimeUnit.MILLISECONDS);
    } else {
      // process immediately when delay is 0.
      handler.run(null);
    }

    // proceed to next action when complete
    future.whenComplete(
        (v, ex) -> {
          if (ex != null) {
            doneFuture.completeExceptionally(ex);
          } else {
            handleActions(nextActions, ctx, frame, doneFuture);
          }
        });
  }

  private class ActionHandler implements TimerTask {

    private final Action action;
    private final ChannelHandlerContext ctx;
    private final Frame frame;
    private final CompletableFuture<Void> doneFuture;

    ActionHandler(
        Action action, ChannelHandlerContext ctx, Frame frame, CompletableFuture<Void> doneFuture) {
      this.action = action;
      this.ctx = ctx;
      this.frame = frame;
      this.doneFuture = doneFuture;
    }

    @Override
    public void run(Timeout timeout) {
      CompletableFuture<Void> future;
      // TODO maybe delegate this logic elsewhere
      if (action instanceof MessageResponseAction) {
        MessageResponseAction mAction = (MessageResponseAction) action;
        future = completable(sendMessage(ctx, frame, mAction.getMessage()));
      } else if (action instanceof DisconnectAction) {
        DisconnectAction cAction = (DisconnectAction) action;
        switch (cAction.getScope()) {
          case CONNECTION:
            future =
                closeConnectionAsync(ctx.channel().remoteAddress(), cAction.getCloseType())
                    .toCompletableFuture()
                    .thenApply(v -> null);
            break;
          default:
            Stream<Node> nodes =
                cAction.getScope() == NODE
                    ? Stream.of(BoundNode.this)
                    : cAction.getScope() == CLUSTER
                        ? getBoundCluster().getNodes().stream()
                        : getDataCenter().getNodes().stream();
            future = closeNodes(nodes, cAction.getCloseType());
            break;
        }
      } else if (action instanceof NoResponseAction) {
        future = new CompletableFuture<>();
        future.complete(null);
      } else {
        logger.warn("Got action {} that we don't know how to handle.", action);
        future = new CompletableFuture<>();
        future.complete(null);
      }

      future.whenComplete(
          (v, t) -> {
            if (t != null) {
              doneFuture.completeExceptionally(t);
            } else {
              doneFuture.complete(v);
            }
          });
    }
  }

  private static CompletableFuture<Void> closeNodes(Stream<Node> nodes, CloseType closeType) {
    return CompletableFuture.allOf(
        nodes
            .map(n -> ((BoundNode) n).closeConnectionsAsync(closeType).toCompletableFuture())
            .collect(Collectors.toList())
            .toArray(new CompletableFuture[] {}));
  }

  private ChannelFuture sendMessage(
      ChannelHandlerContext ctx, Frame requestFrame, Message responseMessage) {
    Frame responseFrame = wrapResponse(requestFrame, responseMessage);
    logger.debug(
        "Sending response for streamId: {} with msg {}",
        responseFrame.streamId,
        responseFrame.message);
    return ctx.writeAndFlush(responseFrame);
  }

  @Override
  public StubStore getStubStore() {
    return stubStore;
  }

  @Override
  public NodeConnectionReport getConnections() {
    ClusterConnectionReport clusterConnectionReport = new ClusterConnectionReport(cluster.getId());
    return clusterConnectionReport.addNode(
        this,
        clientChannelGroup.stream().map(Channel::remoteAddress).collect(Collectors.toList()),
        getAddress());
  }

  @Override
  public CompletionStage<NodeConnectionReport> closeConnectionsAsync(CloseType closeType) {
    NodeConnectionReport report = getConnections();

    return closeChannelGroup(this.clientChannelGroup, closeType).thenApply(v -> report);
  }

  private static CompletableFuture<Void> closeChannelGroup(
      ChannelGroup channelGroup, CloseType closeType) {
    switch (closeType) {
      case DISCONNECT:
        return completable(channelGroup.disconnect());
      default:
        return CompletableFuture.allOf(
            channelGroup
                .stream()
                .map(
                    c -> {
                      CompletableFuture<Void> f;
                      Function<SocketChannel, ChannelFuture> shutdownMethod =
                          closeType == CloseType.SHUTDOWN_READ
                              ? SocketChannel::shutdownInput
                              : SocketChannel::shutdownOutput;
                      if (c instanceof SocketChannel) {
                        f = completable(shutdownMethod.apply((SocketChannel) c));
                      } else {
                        logger.warn(
                            "Got {} request for non-SocketChannel {}, disconnecting instead.",
                            closeType,
                            c);
                        f = completable(c.disconnect());
                      }
                      return f;
                    })
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}));
    }
  }

  @Override
  public CompletionStage<NodeConnectionReport> closeConnectionAsync(
      SocketAddress connection, CloseType type) {
    Optional<Channel> channel =
        this.clientChannelGroup
            .stream()
            .filter(c -> c.remoteAddress().equals(connection))
            .findFirst();

    if (channel.isPresent()) {
      ChannelGroup channelGroup = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);
      channelGroup.add(channel.get());
      ClusterConnectionReport clusterReport = new ClusterConnectionReport(getCluster().getId());
      NodeConnectionReport report =
          clusterReport.addNode(this, Collections.singletonList(connection), getAddress());

      return closeChannelGroup(channelGroup, type).thenApply(f -> report);
    } else {
      CompletableFuture<NodeConnectionReport> failedFuture = new CompletableFuture<>();
      failedFuture.completeExceptionally(new IllegalArgumentException("Not found"));
      return failedFuture;
    }
  }

  @Override
  public Stream<BoundNode> getBoundNodes() {
    return Stream.of(this);
  }

  @Override
  public BoundCluster getBoundCluster() {
    return cluster;
  }

  /**
   * @return The {@link DataCenter} this node belongs to, otherwise null if it does not have one.
   */
  @JsonIgnore
  public BoundDataCenter getDataCenter() {
    Optional<NodeProperties> parent = getParent();
    return parent.map(dc -> (BoundDataCenter) dc).orElse(null);
  }

  @Override
  public Server getServer() {
    return server;
  }

  /**
   * Convenience fluent builder for constructing a prime with a query, where the parameters are
   * inferred by the query
   *
   * @param query The query string to match against.
   * @return builder for this prime.
   */
  private static PrimeBuilder whenWithInferredParams(String query) {
    long posParamCount = query.chars().filter(num -> num == '?').count();

    // Do basic param population for positional types
    HashMap<String, String> paramTypes = new HashMap<>();
    HashMap<String, Object> params = new HashMap<>();
    if (posParamCount > 0) {
      for (int i = 0; i < posParamCount; i++) {
        params.put(Integer.toString(i), "*");
        paramTypes.put(Integer.toString(i), "varchar");
      }
    }
    // Do basic param population for named types
    else {
      List<String> allMatches = new ArrayList<>();
      Pattern p = Pattern.compile("([\\w']+)\\s=\\s:[\\w]+");
      Matcher m = p.matcher(query);
      while (m.find()) {
        allMatches.add(m.group(1));
      }
      for (String match : allMatches) {
        params.put(match, "*");
        paramTypes.put(match, "varchar");
      }
    }
    return when(
        new com.datastax.simulacron.common.request.Query(
            query, Collections.emptyList(), params, paramTypes));
  }
}
