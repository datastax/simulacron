package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.DisconnectAction;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.datastax.oss.protocol.internal.response.result.Void.INSTANCE;
import static com.datastax.simulacron.common.utils.FrameUtils.wrapResponse;
import static com.datastax.simulacron.server.ChannelUtils.completable;

class BoundNode extends Node {

  private static Logger logger = LoggerFactory.getLogger(BoundNode.class);

  private static final Pattern useKeyspacePattern =
      Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE);

  private final ServerBootstrap bootstrap;

  // TODO: Isn't really a good reason for this to be an AtomicReference as if binding fails we don't reset
  // the channel, but leaving it this way for now in case there is a future use case.
  final transient AtomicReference<Channel> channel;

  final transient ChannelGroup clientChannelGroup =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  // TODO: There could be a lot of concurrency issues around simultaneous calls to reject/accept, however
  // in the general case we don't expect it.   Leave this as AtomicReference in case we want to handle it better.
  private final AtomicReference<RejectState> rejectState = new AtomicReference<>(new RejectState());

  private final transient StubStore stubStore;

  enum RejectScope {
    UNBIND, // unbind the channel so can't establish TCP connection
    REJECT_STARTUP // keep channel bound so can establish TCP connection, but doesn't reply to startup.
  }

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
      DataCenter parent,
      ServerBootstrap bootstrap,
      Channel channel,
      StubStore stubStore) {
    super(address, name, id, cassandraVersion, dseVersion, peerInfo, parent);
    this.bootstrap = bootstrap;
    this.channel = new AtomicReference<>(channel);
    this.stubStore = stubStore;
  }

  /**
   * Closes the listening channel for this node. Note that this does not close existing client
   * connections, this can be done using {@link #disconnectConnections()}. To stop listening and
   * close connections, use {@link #close()}.
   *
   * @return future that completes when listening channel is closed.
   */
  CompletableFuture<Node> unbind() {
    logger.debug("Unbinding listener on {}", channel);
    return completable(channel.get().close()).thenApply(v -> this);
  }

  /**
   * Reopens the listening channel for this node. If the channel was already open, has no effect and
   * future completes immediately.
   *
   * @return future that completes when listening channel is reopened.
   */
  CompletableFuture<Node> rebind() {
    if (this.channel.get().isOpen()) {
      // already accepting...
      return CompletableFuture.completedFuture(this);
    }
    CompletableFuture<Node> future = new CompletableFuture<>();
    ChannelFuture bindFuture = bootstrap.bind(this.getAddress());
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              if (channelFuture.isSuccess()) {
                channelFuture.channel().attr(Server.HANDLER).set(this);
                logger.debug("Bound {} to {}", BoundNode.this, channelFuture.channel());
                future.complete(BoundNode.this);
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
   * Closes both the listening channel and all existing client channels.
   *
   * @return future that completes when listening channel and client channels are all closed.
   */
  CompletableFuture<Node> close() {
    return unbind().thenCombine(disconnectConnections(), (n0, n1) -> this);
  }

  /**
   * Disconnects all client channels. Does not close listening interface (see {@link #unbind()} for
   * that).
   *
   * @return future that completes when all client channels are disconnected.
   */
  CompletableFuture<Node> disconnectConnections() {
    return completable(clientChannelGroup.disconnect()).thenApply(v -> this);
  }

  /**
   * Indicates that the node should resume accepting connections.
   *
   * @return future that completes when node is listening again.
   */
  CompletableFuture<Node> acceptNewConnections() {
    logger.debug("Accepting New Connections");
    rejectState.set(new RejectState());
    // Reopen listening interface if not currently open.
    if (!channel.get().isOpen()) {
      return rebind();
    } else {
      return CompletableFuture.completedFuture(this);
    }
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
  CompletableFuture<Node> rejectNewConnections(int after, RejectScope scope) {
    RejectState state;
    if (after <= 0) {
      logger.debug("Rejecting new connections with scope {}", scope);
      state = new RejectState(false, Integer.MIN_VALUE, scope);
    } else {
      logger.debug("Rejecting new connections after {} attempts with scope {}", after, scope);
      state = new RejectState(true, after, scope);
    }
    rejectState.set(state);
    if (after <= 0 && scope == RejectScope.UNBIND) {
      return unbind();
    } else {
      return CompletableFuture.completedFuture(this);
    }
  }

  void handle(ChannelHandlerContext ctx, Frame frame) {
    logger.debug("Got request streamId: {} msg: {}", frame.streamId, frame.message);

    // On receiving a message, first check the stub store to see if there is handling logic for it.
    // If there is, handle each action.
    // Otherwise delegate to default behavior.
    List<Action> actions = stubStore.handle(this, frame);
    if (actions.size() != 0) {
      // TODO: It might be useful to tie behavior to completion of actions but for now this isn't necessary.
      CompletableFuture<Void> future = new CompletableFuture<>();
      handleActions(actions.iterator(), ctx, frame, future);
    } else {
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
            rejectNewConnections(-1, state.scope);
          }
        }
        response = new Ready();
      } else if (frame.message instanceof Options) {
        response = new Supported(new HashMap<>());
      } else if (frame.message instanceof Query) {
        Query query = (Query) frame.message;
        String queryStr = query.query;
        if (queryStr.startsWith("USE") || queryStr.startsWith("use")) {
          Matcher matcher = useKeyspacePattern.matcher(queryStr);
          // should always match.
          assert matcher.matches();
          if (matcher.matches()) {
            String keyspace = matcher.group(1);
            response = new SetKeyspace(keyspace);
          }
        } else {
          response = INSTANCE;
        }
      }
      if (response != null) {
        sendMessage(ctx, frame, response);
      }
    }
  }

  private void handleActions(
      Iterator<Action> nextActions,
      ChannelHandlerContext ctx,
      Frame frame,
      CompletableFuture<Void> doneFuture) {
    if (!nextActions.hasNext()) {
      doneFuture.complete(null);
      return;
    }
    // TODO handle delay
    // TODO maybe delegate this logic elsewhere
    CompletableFuture<Void> future = null;
    Action action = nextActions.next();
    if (action instanceof MessageResponseAction) {
      MessageResponseAction mAction = (MessageResponseAction) action;
      future = completable(sendMessage(ctx, frame, mAction.getMessage()));
    } else if (action instanceof DisconnectAction) {
      DisconnectAction cAction = (DisconnectAction) action;
      switch (cAction.getScope()) {
        case CONNECTION:
          future = completable(ctx.disconnect());
          break;
        case NODE:
          future = disconnectConnections().thenApply(n -> null);
          break;
        case DATACENTER:
          List<CompletableFuture<Void>> futures =
              getDataCenter()
                  .getNodes()
                  .stream()
                  .map(n -> completable(((BoundNode) n).clientChannelGroup.disconnect()))
                  .collect(Collectors.toList());

          future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
          break;
        case CLUSTER:
          futures =
              getCluster()
                  .getNodes()
                  .stream()
                  .map(n -> completable(((BoundNode) n).clientChannelGroup.disconnect()))
                  .collect(Collectors.toList());

          future = CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}));
          break;
      }
    } else {
      logger.warn("Got action {} that we don't know how to handle.", action);
    }

    if (future != null) {
      future.whenComplete(
          (r, ex) -> {
            if (ex != null) {
              doneFuture.completeExceptionally(ex);
            } else {
              handleActions(nextActions, ctx, frame, doneFuture);
            }
          });
    } else {
      handleActions(nextActions, ctx, frame, doneFuture);
    }
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
}
