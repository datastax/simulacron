package com.datastax.simulacron.common.codec;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public interface DataTypeEncoders {

  int protocolVersion();

  default ByteBuffer string(String s) {
    try {
      return ByteBuffer.wrap(s.getBytes("US-ASCII"));
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("Invalid value " + s);
    }
  }

  default ByteBuffer varchar(String s) {
    try {
      return ByteBuffer.wrap(s.getBytes("UTF-8"));
    } catch (UnsupportedEncodingException uee) {
      throw new RuntimeException("Invalid value " + s);
    }
  }

  default ByteBuffer inet(InetAddress i) {
    return ByteBuffer.wrap(i.getAddress());
  }

  default ByteBuffer uuid(UUID u) {
    ByteBuffer buf = ByteBuffer.allocate(16);
    buf.putLong(0, u.getMostSignificantBits());
    buf.putLong(8, u.getLeastSignificantBits());
    return buf;
  }

  <E, C extends Collection<E>> Encoder<C> collectionOf(Encoder<E> encoder);

  default <E> Encoder<List<E>> listOf(Encoder<E> encoder) {
    return collectionOf(encoder);
  }

  default <E> Encoder<Set<E>> setOf(Encoder<E> encoder) {
    return collectionOf(encoder);
  }
}
