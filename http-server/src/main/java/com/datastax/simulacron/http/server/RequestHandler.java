package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.RequestPrime;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;

public class RequestHandler extends StubMapping {
  RequestPrime primedRequest;

  public RequestHandler(RequestPrime primedQuery) {
    this.primedRequest = primedQuery;
  }

  @Override
  public boolean matches(Frame frame) {
    return primedRequest.when.matches(frame);
  }

  private RowsMetadata fetchRowMetadataForParams(Query query) {
    CodecUtils.ColumnSpecBuilder columnBuilder = columnSpecBuilder();
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    for (String key : query.paramTypes.keySet()) {
      RawType type = CodecUtils.getTypeFromName(query.paramTypes.get(key));
      columnMetadata.add(columnBuilder.apply(key, type));
    }
    return new RowsMetadata(columnMetadata, columnMetadata.size(), null, new int[] {0});
  }

  private RowsMetadata fetchRowMetadataForResults(SuccessResult result) {
    if (primedRequest.then instanceof SuccessResult) {
      CodecUtils.ColumnSpecBuilder columnBuilder = columnSpecBuilder();
      List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
      for (String key : result.columnTypes.keySet()) {
        RawType type = CodecUtils.getTypeFromName(result.columnTypes.get(key));
        columnMetadata.add(columnBuilder.apply(key, type));
      }
      return new RowsMetadata(columnMetadata, columnMetadata.size(), null, new int[] {0});
    }
    return null;
  }

  public List<Action> toPreparedAction(Node node, Frame frame, Query query, SuccessResult result) {

    ByteBuffer b = ByteBuffer.allocate(4);
    b.putInt(query.getQueryId());
    Prepared preparedResponse =
        new Prepared(
            b.array(), fetchRowMetadataForParams(query), fetchRowMetadataForResults(result));
    MessageResponseAction action = new MessageResponseAction(preparedResponse);
    return Collections.singletonList(action);
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    if (frame.message instanceof Prepare) {
      if (primedRequest.when instanceof Query && primedRequest.then instanceof SuccessResult) {
        return this.toPreparedAction(
            node, frame, (Query) primedRequest.when, (SuccessResult) primedRequest.then);
      }
    }
    return primedRequest.then.toActions(node, frame);
  }
}
