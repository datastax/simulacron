package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.fasterxml.jackson.databind.JavaType;

import java.nio.ByteBuffer;

public interface Codec<T> {

  JavaType getJavaType();

  RawType getCqlType();

  ByteBuffer encode(T input);

  T decode(ByteBuffer input);

  T toNativeType(Object input);

  default ByteBuffer encodeObject(Object input) {
    return encode(toNativeType(input));
  }
}
