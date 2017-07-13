package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.codec.CodecUtils;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;
import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecs;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;

public class EmptyReturnMetadataHandler extends StubMapping implements InternalStubMapping {

  private List<String> queries = new ArrayList<>();

  public EmptyReturnMetadataHandler(String matchingQuery) {
    queries.add(matchingQuery);
  }

  @Override
  public boolean matches(Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      String queryStr = query.query;
      return queries.contains(queryStr);
    }
    return false;
  }

  @Override
  public List<Action> getActions(AbstractNode node, Frame frame) {
    if (frame.message instanceof Query) {
      Queue<List<ByteBuffer>> peerRows = new ArrayDeque<>();
      Rows rows = new Rows(buildEmptyRowsMetadata(), peerRows);
      MessageResponseAction action = new MessageResponseAction(rows);
      return Collections.singletonList(action);
    }
    return Collections.emptyList();
  }

  private RowsMetadata buildEmptyRowsMetadata() {
    CodecUtils.ColumnSpecBuilder systemPeers =
        columnSpecBuilder("whatever_keyspace", "whatever_table");
    List<ColumnSpec> systemPeersSpecs = columnSpecs(systemPeers.apply("key", primitive(INT)));

    return new RowsMetadata(systemPeersSpecs, null, new int[] {0});
  }
}
