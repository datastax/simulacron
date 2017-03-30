package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.PrimitiveCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.CharsetUtil;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

public class ByteBufCodec implements PrimitiveCodec<ByteBuf> {

  private final ByteBufAllocator alloc;

  ByteBufCodec() {
    this(ByteBufAllocator.DEFAULT);
  }

  ByteBufCodec(ByteBufAllocator alloc) {
    this.alloc = alloc;
  }

  @Override
  public ByteBuf allocate(int size) {
    return alloc.buffer(size);
  }

  @Override
  public void release(ByteBuf toRelease) {
    toRelease.release();
  }

  @Override
  public int sizeOf(ByteBuf toMeasure) {
    return toMeasure.readableBytes();
  }

  @Override
  public ByteBuf concat(ByteBuf left, ByteBuf right) {
    return new CompositeByteBuf(alloc, alloc.isDirectBufferPooled(), 2, left, right);
  }

  @Override
  public byte readByte(ByteBuf source) {
    return source.readByte();
  }

  @Override
  public int readInt(ByteBuf source) {
    return source.readInt();
  }

  @Override
  public InetSocketAddress readInet(ByteBuf source) {
    InetAddress addr = readInetAddr(source);
    int port = readInt(source);
    return new InetSocketAddress(addr, port);
  }

  @Override
  public InetAddress readInetAddr(ByteBuf source) {
    int len = readByte(source);
    byte[] addr = new byte[len];
    source.getBytes(source.readerIndex(), addr, 0, len);
    try {
      return InetAddress.getByAddress(addr);
    } catch (UnknownHostException uhe) {
      throw new IllegalArgumentException(uhe);
    }
  }

  @Override
  public long readLong(ByteBuf source) {
    return source.readLong();
  }

  @Override
  public int readUnsignedShort(ByteBuf source) {
    return source.readUnsignedShort();
  }

  @Override
  public ByteBuffer readBytes(ByteBuf source) {
    int len = readInt(source);
    if (len < 0) {
      return null;
    }

    ByteBuf slice = source.readSlice(len);
    // if direct byte buffer, return underlying nioBuffer.
    if (slice.isDirect()) {
      return slice.nioBuffer();
    }
    // otherwise copy to a byte array and wrap it.
    final byte[] out = new byte[slice.readableBytes()];
    source.getBytes(source.readerIndex(), out, 0, len);
    return ByteBuffer.wrap(out);
  }

  @Override
  public byte[] readShortBytes(ByteBuf source) {
    int len = readUnsignedShort(source);
    byte[] out = new byte[len];
    source.getBytes(source.readerIndex(), out, 0, len);
    return out;
  }

  @Override
  public String readString(ByteBuf source) {
    int len = readUnsignedShort(source);
    String str = source.toString(source.readerIndex(), len, CharsetUtil.UTF_8);
    source.readerIndex(source.readerIndex() + len);
    return str;
  }

  @Override
  public String readLongString(ByteBuf source) {
    int len = readInt(source);
    String str = source.toString(source.readerIndex(), len, CharsetUtil.UTF_8);
    source.readerIndex(source.readerIndex() + len);
    return str;
  }

  @Override
  public void writeByte(byte b, ByteBuf dest) {
    dest.writeByte(b);
  }

  @Override
  public void writeInt(int i, ByteBuf dest) {
    dest.writeInt(i);
  }

  @Override
  public void writeInet(InetSocketAddress inetSocketAddress, ByteBuf source) {
    writeInetAddr(inetSocketAddress.getAddress(), source);
    writeInt(inetSocketAddress.getPort(), source);
  }

  @Override
  public void writeInetAddr(InetAddress address, ByteBuf dest) {
    byte[] addr = address.getAddress();
    dest.writeByte(addr.length);
    dest.writeBytes(addr);
  }

  @Override
  public void writeLong(long l, ByteBuf dest) {
    dest.writeLong(l);
  }

  @Override
  public void writeUnsignedShort(int i, ByteBuf dest) {
    dest.writeShort(i);
  }

  @Override
  public void writeString(String s, ByteBuf dest) {
    byte[] data = s.getBytes(CharsetUtil.UTF_8);
    writeUnsignedShort(data.length, dest);
    dest.writeBytes(data);
  }

  @Override
  public void writeLongString(String s, ByteBuf dest) {
    byte[] data = s.getBytes(CharsetUtil.UTF_8);
    writeInt(data.length, dest);
    dest.writeBytes(data);
  }

  @Override
  public void writeBytes(ByteBuffer bytes, ByteBuf dest) {
    if (bytes == null) {
      writeInt(-1, dest);
    } else {
      writeInt(bytes.remaining(), dest);
      dest.writeBytes(bytes.duplicate());
    }
  }

  @Override
  public void writeShortBytes(byte[] bytes, ByteBuf dest) {
    if (bytes == null) {
      writeUnsignedShort(-1, dest);
    } else {
      writeUnsignedShort(bytes.length, dest);
      dest.writeBytes(bytes);
    }
  }
}
