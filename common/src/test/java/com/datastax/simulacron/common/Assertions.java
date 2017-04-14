package com.datastax.simulacron.common;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.result.Rows;

import java.nio.ByteBuffer;

public class Assertions extends org.assertj.core.api.Assertions {

  public static MessageAssert assertThat(Message message) {
    return new MessageAssert(message);
  }

  public static RowsAssert assertThat(Rows rows) {
    return new RowsAssert(rows);
  }

  public static ByteBufferAssert assertThat(ByteBuffer buf) {
    return new ByteBufferAssert(buf);
  }
}
