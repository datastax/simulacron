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

import com.datastax.oss.protocol.internal.Frame;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.slf4j.MDC;

public class RequestHandler extends ChannelInboundHandlerAdapter {

  private BoundNode node;

  public RequestHandler(BoundNode node) {
    this.node = node;
  }

  @Override
  public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
    MDC.put("node", node.getId().toString());

    try {
      if (msg instanceof Frame) {
        Frame frame = (Frame) msg;
        node.handle(ctx, frame);
      } else if (msg instanceof UnsupportedProtocolVersionMessage) {
        UnsupportedProtocolVersionMessage umsg = (UnsupportedProtocolVersionMessage) msg;
        node.handle(ctx, umsg);
      } else {
        throw new IllegalArgumentException("Received Invalid message into handler: " + msg);
      }
    } finally {
      MDC.remove("node");
    }
  }
}
