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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.ChannelGroupFuture;
import io.netty.channel.group.ChannelGroupFutureListener;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.datastax.oss.protocol.internal.response.result.Void.INSTANCE;
import static com.datastax.simulacron.common.utils.FrameUtils.wrapResponse;

class BoundNode extends Node {

  private static Logger logger = LoggerFactory.getLogger(BoundNode.class);

  private static final Pattern useKeyspacePattern =
      Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE);

  final transient Channel channel;

  final transient ChannelGroup clientChannelGroup =
      new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

  private final transient StubStore stubStore;

  BoundNode(
      SocketAddress address,
      String name,
      Long id,
      String cassandraVersion,
      Map<String, Object> peerInfo,
      DataCenter parent,
      Channel channel,
      StubStore stubStore) {
    super(address, name, id, cassandraVersion, peerInfo, parent);
    this.channel = channel;
    this.stubStore = stubStore;
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

  /**
   * Convenience method to convert a {@link ChannelFuture} into a {@link CompletableFuture}
   *
   * @param future future to convert.
   * @return converted future.
   */
  private static CompletableFuture<Void> completable(ChannelFuture future) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    future.addListener(
        (ChannelFutureListener)
            future1 -> {
              if (future1.isSuccess()) {
                cf.complete(null);
              } else {
                cf.completeExceptionally(future1.cause());
              }
            });
    return cf;
  }

  private static CompletableFuture<Void> completable(ChannelGroupFuture future) {
    CompletableFuture<Void> cf = new CompletableFuture<>();
    future.addListener(
        (ChannelGroupFutureListener)
            future1 -> {
              if (future1.isSuccess()) {
                cf.complete(null);
              } else {
                cf.completeExceptionally(future1.cause());
              }
            });
    return cf;
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
          future = completable(clientChannelGroup.disconnect());
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
