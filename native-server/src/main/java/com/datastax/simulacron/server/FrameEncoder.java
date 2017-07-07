package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

public class FrameEncoder extends MessageToByteEncoder<Frame> {

  private final FrameCodec<ByteBuf> frameCodec;

  FrameEncoder(FrameCodec<ByteBuf> frameCodec) {
    this.frameCodec = frameCodec;
  }

  @Override
  protected void encode(ChannelHandlerContext ctx, Frame msg, ByteBuf out) throws Exception {
    ByteBuf data = frameCodec.encode(msg);
    out.writeBytes(data);
    data.release();
  }
}
