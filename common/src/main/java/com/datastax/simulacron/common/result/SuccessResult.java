package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;

public class SuccessResult extends Result {
  public final List<Map<String, Object>> rows;
  public final Map<String, String> columnTypes;

  public SuccessResult(List<Map<String, Object>> rows, Map<String, String> columnTypes) {
    this(rows, columnTypes, 0);
  }

  public SuccessResult(
      List<Map<String, Object>> rows, Map<String, String> columnTypes, long delayInMs) {
    super(delayInMs);
    this.rows = rows;
    this.columnTypes = columnTypes;
  }

  @Override
  public List<Action> toActions(Node node, Frame frame) {
    CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
    //This will return all the rows specified in the query, along with any corresponding metadata about the row
    boolean meta_constructed = false;
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    Queue<List<ByteBuffer>> rows = new LinkedList<List<ByteBuffer>>();

    for (Map<String, Object> row : this.rows) {
      List<ByteBuffer> rowByteBuffer = new LinkedList<ByteBuffer>();
      CodecUtils.ColumnSpecBuilder columnBuilder = columnSpecBuilder();
      //Iterate over all the rows and create column meta data if needed
      for (String key : row.keySet()) {
        RawType type = CodecUtils.getTypeFromName(columnTypes.get(key));
        if (!meta_constructed) {
          columnMetadata.add(columnBuilder.apply(key, type));
        }
        rowByteBuffer.add(mapper.codecFor(type).encodeObject(row.get(key)));
      }
      meta_constructed = true;
      rows.add(rowByteBuffer);
    }
    RowsMetadata rowMetadata = new RowsMetadata(columnMetadata, null, new int[] {0});
    MessageResponseAction action =
        new MessageResponseAction(new Rows(rowMetadata, rows), getDelayInMs());
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
