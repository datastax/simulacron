package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.MDC;

class RequestHandler extends ChannelInboundHandlerAdapter {

  private BoundNode node;

  RequestHandler(BoundNode node) {
    this.node = node;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    MDC.put("node", node.getId().toString());

    try {
      @SuppressWarnings("unchecked")
      Frame frame = (Frame) msg;
      node.handle(ctx, frame);
    } finally {
      MDC.remove("node");
    }
  }
}
