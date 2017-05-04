package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.codec.CodecUtils;

import java.nio.ByteBuffer;
import java.util.*;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;
import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecs;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;

public class EmptyReturnMetadataHandler implements StubMapping {

  private List<String> queries = new ArrayList<>();

  public EmptyReturnMetadataHandler(String matchingQuery) {
    queries.add(matchingQuery);
  }

  @Override
  public boolean matches(Node node, Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      String queryStr = query.query;
      return queries.contains(queryStr);
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    /*
    MessageResponseAction action = new MessageResponseAction(INSTANCE);
    return Collections.singletonList(action);
    */
    Queue<List<ByteBuffer>> peerRows = new ArrayDeque<>();
    Rows rows = new Rows(buildEmptyRowsMetadata(node), peerRows);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  private RowsMetadata buildEmptyRowsMetadata(Node node) {
    CodecUtils.ColumnSpecBuilder systemPeers =
        columnSpecBuilder("whatever_keyspace", "whatever_table");
    List<ColumnSpec> systemPeersSpecs = columnSpecs(systemPeers.apply("key", primitive(INT)));

    return new RowsMetadata(systemPeersSpecs, null, new int[] {0});
  }
}
