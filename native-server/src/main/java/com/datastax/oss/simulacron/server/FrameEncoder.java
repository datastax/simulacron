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
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import java.util.List;

public class FrameEncoder extends MessageToMessageEncoder<Frame> {

  private final FrameCodec<ByteBuf> frameCodec;

  FrameEncoder(FrameCodec<ByteBuf> frameCodec) {
    this.frameCodec = frameCodec;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Frame msg, List<Object> out) throws Exception {
    out.add(frameCodec.encode(msg));
  }
}
