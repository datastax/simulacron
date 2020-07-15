/*
 * Copyright DataStax, Inc.
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
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class TestFrameDecoder extends LengthFieldBasedFrameDecoder {
  private final FrameCodec<ByteBuf> frameCodec;

  private static final int MAX_FRAME_LENGTH = 256 * 1024 * 1024; // 256 MB
  private static final int HEADER_LENGTH =
      5; // size of the header = version (1) + flags (1) + stream id (2) + opcode (1)
  private static final int LENGTH_FIELD_LENGTH = 4; // length of the length field.

  TestFrameDecoder(FrameCodec<ByteBuf> frameCodec) {
    super(MAX_FRAME_LENGTH, HEADER_LENGTH, LENGTH_FIELD_LENGTH, 0, 0, true);

    this.frameCodec = frameCodec;
  }

  @Override
  protected Frame decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
    if (buffer.readableBytes() < HEADER_LENGTH) return null;
    ByteBuf contents = (ByteBuf) super.decode(ctx, buffer);
    if (contents == null) return null;
    return frameCodec.decode(contents);
  }

  @Override
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    // use slice instead of retainedSlice (what super does) so don't need to release later.
    return buffer.slice(index, length);
  }
}
