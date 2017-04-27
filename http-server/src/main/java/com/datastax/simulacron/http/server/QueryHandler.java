package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.CqlMapper;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;

public class QueryHandler implements StubMapping {
  QueryPrime primedQuery;

  public QueryHandler(QueryPrime primedQuery) {
    this.primedQuery = primedQuery;
  }

  @Override
  public boolean matches(Node node, Frame frame) {

    if (frame.message instanceof Query) {
      //If the query expected matches the primed query example and CL levels match. Return true;
      Query query = (Query) frame.message;
      if (primedQuery.when.query.equals(query.query)) {

        ConsistencyLevel level = ConsistencyLevel.fromCode(query.options.consistency);
        //NOTE: Absent CL level means it will match all CL levels
        if (primedQuery.when.consistencyEnum.contains(level)
            || primedQuery.when.consistencyEnum.size() == 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {

    CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
    //This will return all the rows specified in the query, along with any corresponding metadata about the row
    boolean meta_constructed = false;
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    Queue<List<ByteBuffer>> rows = new LinkedList<List<ByteBuffer>>();
    for (Map<String, Object> row : primedQuery.then.rows) {

      List<ByteBuffer> rowByteBuffer = new LinkedList<ByteBuffer>();
      CodecUtils.ColumnSpecBuilder columnBuilder = columnSpecBuilder();
      //Iterate over all the rows and create column meta data if needed
      for (String key : row.keySet()) {
        RawType type = getTypeForRow(key);
        if (!meta_constructed) {
          columnMetadata.add(columnBuilder.apply(key, type));
        }
        rowByteBuffer.add(mapper.codecFor(type).encodeObject(row.get(key)));
      }
      meta_constructed = true;
      rows.add(rowByteBuffer);
    }
    RowsMetadata rowMetadata = new RowsMetadata(columnMetadata, null, new int[] {0});
    MessageResponseAction action = new MessageResponseAction(new Rows(rowMetadata, rows));
    return Collections.singletonList(action);
  }

  private RawType getTypeForRow(Object key) {
    String type = primedQuery.then.column_types.get(key);
    return CodecUtils.getTypeFromName(type);
  }
}
