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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;

public class SuccessResult extends Result {
  @JsonProperty("rows")
  public final List<Map<String, Object>> rows;

  @JsonProperty("column_types")
  public final Map<String, String> columnTypes;

  public SuccessResult(List<Map<String, Object>> rows, Map<String, String> columnTypes)
      throws Exception {
    this(rows, columnTypes, 0);
  }

  @JsonCreator
  public SuccessResult(
      @JsonProperty("rows") List<Map<String, Object>> rows,
      @JsonProperty("column_types") Map<String, String> columnTypes,
      @JsonProperty("delay_in_ms") long delayInMs)
      throws Exception {
    super(delayInMs);
    if ((rows != null) ^ (columnTypes != null)) {
      throw new Exception("Both \"rows\" and \"columnTypes\" are required or none of them");
    } else if (rows == null) {
      this.rows = new ArrayList<>();
      this.columnTypes = new HashMap<>();
    } else {
      this.rows = rows;
      this.columnTypes = columnTypes;
    }
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
