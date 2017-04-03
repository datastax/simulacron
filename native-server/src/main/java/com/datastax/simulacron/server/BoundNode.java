package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.Ready;
import com.datastax.oss.protocol.internal.response.Supported;
import com.datastax.oss.protocol.internal.response.result.SetKeyspace;
import com.datastax.oss.protocol.internal.response.result.Void;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BoundNode extends Node {

  private static final Map<String, ByteBuffer> emptyCustomPayload =
      Collections.unmodifiableMap(new HashMap<>());

  private static Logger logger = LoggerFactory.getLogger(BoundNode.class);

  private static final Pattern useKeyspacePattern =
      Pattern.compile("\\s*use\\s+(.*)$", Pattern.CASE_INSENSITIVE);

  transient Channel channel;

  public BoundNode(
      SocketAddress address,
      String name,
      UUID id,
      String cassandraVersion,
      Map<String, Object> peerInfo,
      DataCenter parent,
      Channel channel) {
    super(address, name, id, cassandraVersion, peerInfo, parent);
    this.channel = channel;
  }

  public CompletableFuture<Void> handle(
      ChannelHandlerContext ctx, Frame frame, FrameCodec<ByteBuf> frameCodec) {
    logger.info("Got request streamId: {} msg: {}", frame.streamId, frame.message);
    CompletableFuture<Void> f = new CompletableFuture<>();

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
        response = Void.INSTANCE;
      }
    }

    if (response != null) {
      Frame responseFrame =
          new Frame(
              frame.protocolVersion,
              frame.beta,
              frame.streamId,
              false,
              null,
              emptyCustomPayload,
              Collections.emptyList(),
              response);
      logger.info(
          "Sending response for streamId: {} with msg {}",
          responseFrame.streamId,
          responseFrame.message);
      ByteBuf rsp = frameCodec.encode(responseFrame);
      ChannelFuture cf = ctx.writeAndFlush(rsp);
      cf.addListener(__ -> f.complete(null));
    } else {
      f.complete(null);
    }
    return f;
  }
}
