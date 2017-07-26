/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common;

import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.simulacron.common.codec.CqlMapper;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class RowsAssert extends MessageAssert<RowsAssert, Rows> {
  RowsAssert(Rows actual) {
    super(actual, RowsAssert.class);
  }

  public RowsAssert hasRows(int count) {
    assertThat(actual.getData()).hasSize(count);
    return this;
  }

  public RowsAssert hasColumnSpecs(int count) {
    assertThat(actual.getMetadata().columnSpecs).hasSize(count);
    return this;
  }

  public <T> RowsAssert hasColumn(int row, int column, T expectedValue) {
    CqlMapper mapper = CqlMapper.forVersion(4);
    List<ByteBuffer> data = new ArrayList<>(actual.getData()).get(row);
    assertThat(
            mapper
                .codecFor(actual.getMetadata().columnSpecs.get(column).type)
                .decode(data.get(column)))
        .isEqualTo(expectedValue);
    return this;
  }
}
