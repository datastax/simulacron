package com.datastax.simulacron.common;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.result.Rows;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageAssert<S extends AbstractAssert<S, A>, A extends Message>
    extends AbstractAssert<S, A> {

  MessageAssert(A actual) {
    this(actual, MessageAssert.class);
  }

  MessageAssert(A actual, Class<?> selfType) {
    super(actual, selfType);
  }

  public RowsAssert isRows() {
    assertThat(actual).isInstanceOf(Rows.class);
    return new RowsAssert((Rows) actual);
  }
}
