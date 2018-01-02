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
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FrameDecoder extends LengthFieldBasedFrameDecoder {
  private static final Logger logger = LoggerFactory.getLogger(FrameDecoder.class);
  private final FrameCodecWrapper frameCodec;

  private static final int MAX_FRAME_LENGTH = 256 * 1024 * 1024; // 256 MB
  private static final int HEADER_LENGTH =
      5; // size of the header = version (1) + flags (1) + stream id (2) + opcode (1)
  private static final int LENGTH_FIELD_LENGTH = 4; // length of the length field.

  private boolean isFirstResponse = true;

  FrameDecoder(FrameCodecWrapper frameCodec) {
    super(MAX_FRAME_LENGTH, HEADER_LENGTH, LENGTH_FIELD_LENGTH, 0, 0, true);

    this.frameCodec = frameCodec;
  }

  @Override
  protected Frame decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
    int startIndex = buffer.readerIndex();
    if (isFirstResponse) {
      // Must read at least protocol v1/v2 header (see below)
      if (buffer.readableBytes() < 8) {
        return null;
      }
      isFirstResponse = false;

      // Special case for obsolete protocol versions (< v3): the stream id is a a byte instead
      // of a short, so we need to parse it by hand and return an error.
      int protocolVersion = buffer.getByte(startIndex) & 0x7F;
      if (protocolVersion < 3) {
        byte streamId = buffer.getByte(startIndex + 2);
        int length = buffer.getInt(startIndex + 4);
        // We don't need a full-blown decoder, just to signal the protocol error. So discard the
        // incoming data and spoof a server-side protocol error.
        if (buffer.readableBytes() < 8 + length) {
          return null; // keep reading until we can discard the whole message at once
        } else {
          buffer.readerIndex(startIndex + 8 + length);
        }
        String message = "Invalid or unsupported protocol version";
        // 8 for header, 4 for error code, 2 for message length + message
        ByteBuf buf = ctx.alloc().buffer(14 + message.length());
        buf.writeByte(protocolVersion & 0x7F);
        buf.writeByte(0);
        buf.writeByte(streamId);
        buf.writeByte(0); // error opcode
        buf.writeInt(6 + message.length()); // frame length
        buf.writeInt(0xA); // protocol error;
        buf.writeShort(message.length());
        buf.writeBytes(message.getBytes("UTF-8"));
        ctx.writeAndFlush(buf);
        return null;
      }
    }

    if (buffer.readableBytes() < HEADER_LENGTH) return null;
    ByteBuf contents = (ByteBuf) super.decode(ctx, buffer);
    if (contents == null) return null;

    // handle case where protocol version is greater than what is supported.
    int protocolVersion = contents.getByte(contents.readerIndex()) & 0x7F;
    if (!frameCodec.getSupportedProtocolVersions().contains(protocolVersion)) {
      logger.warn(
          "Received message with unsupported protocol version {}, sending back protocol error.",
          protocolVersion);

      int streamId = contents.getShort(contents.readerIndex() + 2);
      Message message =
          new Error(
              ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
              "Invalid or unsupported protocol version");

      // Respond with max protocol version supported.
      Frame frame =
          new Frame(
              Collections.max(frameCodec.getSupportedProtocolVersions()),
              false,
              streamId,
              false,
              null,
              FrameUtils.emptyCustomPayload,
              Collections.emptyList(),
              message);
      ctx.writeAndFlush(frameCodec.encode(frame));
      int length = contents.getInt(contents.readerIndex() + 5);
      // discard the frame.
      contents.skipBytes(9 + length);
      return null;
    }
    return frameCodec.decode(contents);
  }

  @Override
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    // use slice instead of retainedSlice (what super does) so don't need to release later.
    return buffer.slice(index, length);
  }
}
