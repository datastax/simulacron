package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.ProtocolConstants;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.simulacron.common.utils.FrameUtils;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class FrameDecoder extends LengthFieldBasedFrameDecoder {
  private static final Logger logger = LoggerFactory.getLogger(FrameDecoder.class);
  private final FrameCodec<ByteBuf> frameCodec;

  private static final int MAX_FRAME_LENGTH = 256 * 1024 * 1024; // 256 MB
  private static final int HEADER_LENGTH =
      5; // size of the header = version (1) + flags (1) + stream id (2) + opcode (1)
  private static final int LENGTH_FIELD_LENGTH = 4; // length of the length field.

  FrameDecoder(FrameCodec<ByteBuf> frameCodec) {
    super(MAX_FRAME_LENGTH, HEADER_LENGTH, LENGTH_FIELD_LENGTH, 0, 0, true);

    this.frameCodec = frameCodec;
  }

  @Override
  protected Frame decode(ChannelHandlerContext ctx, ByteBuf buffer) throws Exception {
    if (buffer.readableBytes() < HEADER_LENGTH) return null;
    ByteBuf contents = (ByteBuf) super.decode(ctx, buffer);
    if (contents == null) return null;

    // handle case where protocol version is greater than what is supported.
    int protocolVersion = contents.getByte(contents.readerIndex()) & 0x7F;
    if (protocolVersion > 5) {
      logger.warn(
          "Received message with unsupported protocol version {}, sending back protocol error.",
          protocolVersion);

      int streamId = contents.getShort(contents.readerIndex() + 2);
      Message message =
          new Error(
              ProtocolConstants.ErrorCode.PROTOCOL_ERROR,
              "Invalid or unsupported protocol version");

      Frame frame =
          new Frame(
              4,
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
