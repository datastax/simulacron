/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.codec.CodecUtils;
import com.datastax.oss.simulacron.common.codec.CqlMapper;
import com.datastax.oss.simulacron.common.stubbing.Action;
import com.datastax.oss.simulacron.common.stubbing.MessageResponseAction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

public class SuccessResult extends Result {
  @JsonProperty("rows")
  public final List<LinkedHashMap<String, Object>> rows;

  @JsonProperty("column_types")
  public final LinkedHashMap<String, String> columnTypes;

  public SuccessResult(
      List<LinkedHashMap<String, Object>> rows, LinkedHashMap<String, String> columnTypes) {
    this(rows, columnTypes, 0, null);
  }

  @JsonCreator
  public SuccessResult(
      @JsonProperty("rows") List<LinkedHashMap<String, Object>> rows,
      @JsonProperty("column_types") LinkedHashMap<String, String> columnTypes,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    super(delayInMs, ignoreOnPrepare);
    if ((rows != null) ^ (columnTypes != null)) {
      throw new IllegalArgumentException(
          "Both \"rows\" and \"columnTypes\" are required or none of them");
    } else if (rows == null) {
      this.rows = new ArrayList<>();
      this.columnTypes = new LinkedHashMap<>();
    } else {
      this.rows = rows;
      this.columnTypes = columnTypes;
    }
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
    // This will return all the rows specified in the query, along with any corresponding metadata
    // about the row
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    CodecUtils.ColumnSpecBuilder columnBuilder = CodecUtils.columnSpecBuilder();
    Queue<List<ByteBuffer>> rows = new LinkedList<>();

    // Populate column metadata even if there are no rows.
    if (this.rows.isEmpty()) {
      for (Map.Entry<String, String> columnType : columnTypes.entrySet()) {
        columnMetadata.add(
            columnBuilder.apply(
                columnType.getKey(), CodecUtils.getTypeFromName(columnType.getValue())));
      }
    } else {
      boolean metaConstructed = false;
      for (Map<String, Object> row : this.rows) {
        List<ByteBuffer> rowByteBuffer = new LinkedList<>();
        // Iterate over all the rows and create column meta data if needed
        for (String key : row.keySet()) {
          RawType type = CodecUtils.getTypeFromName(columnTypes.get(key));
          if (!metaConstructed) {
            columnMetadata.add(columnBuilder.apply(key, type));
          }
          rowByteBuffer.add(mapper.codecFor(type).encodeObject(row.get(key)));
        }
        metaConstructed = true;
        rows.add(rowByteBuffer);
      }
    }
    RowsMetadata rowMetadata = new RowsMetadata(columnMetadata, null, new int[] {0}, null);
    MessageResponseAction action =
        new MessageResponseAction(new DefaultRows(rowMetadata, rows), getDelayInMs());
    return Collections.singletonList(action);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    SuccessResult that = (SuccessResult) o;

    if (!rows.equals(that.rows)) return false;
    return columnTypes.equals(that.columnTypes);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + rows.hashCode();
    result = 31 * result + columnTypes.hashCode();
    return result;
  }
}
