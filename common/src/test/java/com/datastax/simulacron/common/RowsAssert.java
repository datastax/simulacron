package com.datastax.simulacron.common;

import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.simulacron.common.codec.CqlMapper;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RowsAssert extends MessageAssert<RowsAssert, Rows> {
  RowsAssert(Rows actual) {
    super(actual, RowsAssert.class);
  }

  public RowsAssert hasRows(int count) {
    assertThat(actual.data).hasSize(count);
    return this;
  }

  public RowsAssert hasColumnSpecs(int count) {
    assertThat(actual.metadata.columnSpecs).hasSize(count);
    return this;
  }

  public <T> RowsAssert hasColumn(int row, int column, T expectedValue) {
    CqlMapper mapper = CqlMapper.forVersion(4);
    List<ByteBuffer> data = new ArrayList<>(actual.data).get(row);
    assertThat(
            mapper.codecFor(actual.metadata.columnSpecs.get(column).type).decode(data.get(column)))
        .isEqualTo(expectedValue);
    return this;
  }
}
