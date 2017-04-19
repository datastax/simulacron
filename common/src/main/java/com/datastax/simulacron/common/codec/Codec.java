package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.fasterxml.jackson.databind.JavaType;

import java.nio.ByteBuffer;
import java.util.Optional;

public interface Codec<T> {

  JavaType getJavaType();

  RawType getCqlType();

  ByteBuffer encode(T input);

  T decode(ByteBuffer input);

  Optional<T> toNativeType(Object input);

  default Optional<ByteBuffer> encodeObject(Object input) {
    return toNativeType(input).map(this::encode);
  }
}
