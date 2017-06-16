package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.RequestPrime;
import com.datastax.simulacron.common.cluster.Scope;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.result.SuccessResult;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static com.datastax.simulacron.common.codec.CodecUtils.columnSpecBuilder;

public class Prime extends StubMapping {
  private final RequestPrime primedRequest;

  public Prime(RequestPrime primedQuery, Scope scope) {
    this.setScope(scope);
    this.primedRequest = primedQuery;
  }

  public RequestPrime getPrimedRequest() {
    return primedRequest;
  }

  @Override
  public boolean matches(Frame frame) {
    return primedRequest.when.matches(frame);
  }

  private RowsMetadata fetchRowMetadataForParams(Query query) {
    CodecUtils.ColumnSpecBuilder columnBuilder = columnSpecBuilder();
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    if (query.paramTypes != null) {
      for (String key : query.paramTypes.keySet()) {
        RawType type = CodecUtils.getTypeFromName(query.paramTypes.get(key));
        columnMetadata.add(columnBuilder.apply(key, type));
      }
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

  public Prepared toPrepared() {
    if (this.primedRequest.when instanceof Query
        && this.primedRequest.then instanceof SuccessResult) {
      Query query = (Query) this.primedRequest.when;
      SuccessResult result = (SuccessResult) this.primedRequest.then;
      ByteBuffer b = ByteBuffer.allocate(4);
      b.putInt(query.getQueryId());
      return new Prepared(
          b.array(), fetchRowMetadataForParams(query), fetchRowMetadataForResults(result));
    }
    return null;
  }

  public List<Action> toPreparedAction() {
    Prepared preparedResponse = toPrepared();
    MessageResponseAction action = new MessageResponseAction(preparedResponse);
    return Collections.singletonList(action);
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    if (frame.message instanceof Prepare) {
      if (primedRequest.when instanceof Query && primedRequest.then instanceof SuccessResult) {
        return this.toPreparedAction();
      }
    }
    return primedRequest.then.toActions(node, frame);
  }
}
