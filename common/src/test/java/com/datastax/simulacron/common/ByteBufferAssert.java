package com.datastax.simulacron.common;

import com.datastax.oss.protocol.internal.util.Bytes;
import org.assertj.core.api.AbstractAssert;

import java.nio.ByteBuffer;

import static com.datastax.simulacron.common.Assertions.assertThat;

public class ByteBufferAssert extends AbstractAssert<ByteBufferAssert, ByteBuffer> {

  protected ByteBufferAssert(ByteBuffer actual) {
    super(actual, ByteBufferAssert.class);
  }

  public ByteBufferAssert hasBytes(String expected) {
    String byteStr = Bytes.toHexString(actual);

    assertThat(byteStr).isEqualTo(expected);
    return this;
  }
}
