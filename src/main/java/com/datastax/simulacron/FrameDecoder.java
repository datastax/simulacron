package com.datastax.simulacron;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;

public class FrameDecoder extends LengthFieldBasedFrameDecoder {
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

    // Parse protocol version
    int version = buffer.getByte(0);
    // first bit is the "direction" of the frame (0 = request)
    assert (version & 0x80) == 0 : "Frame decoder should only receive request frames";

    return frameCodec.decode(contents);
  }
}
