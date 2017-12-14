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
package com.datastax.oss.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.Prepared;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.CodecUtils;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.ErrorResult;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.VoidResult;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class Prime extends StubMapping {
  private final RequestPrime primedRequest;

  public Prime(RequestPrime primedQuery) {
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
    CodecUtils.ColumnSpecBuilder columnBuilder = CodecUtils.columnSpecBuilder();
    List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
    if (query.paramTypes != null) {
      for (String key : query.paramTypes.keySet()) {
        RawType type = CodecUtils.getTypeFromName(query.paramTypes.get(key));
        columnMetadata.add(columnBuilder.apply(key, type));
      }
    }
    return new RowsMetadata(columnMetadata, null, new int[] {0}, null);
  }

  private RowsMetadata fetchRowMetadataForResults(SuccessResult result) {
    if (primedRequest.then instanceof SuccessResult) {
      CodecUtils.ColumnSpecBuilder columnBuilder = CodecUtils.columnSpecBuilder();
      List<ColumnSpec> columnMetadata = new LinkedList<ColumnSpec>();
      for (String key : result.columnTypes.keySet()) {
        RawType type = CodecUtils.getTypeFromName(result.columnTypes.get(key));
        columnMetadata.add(columnBuilder.apply(key, type));
      }
      return new RowsMetadata(columnMetadata, null, new int[] {0}, null);
    }
    return null;
  }

  private static final RowsMetadata rowMetadataForVoid =
      new RowsMetadata(new LinkedList<ColumnSpec>(), null, new int[] {0}, null);

  public Prepared toPrepared() {
    if (this.primedRequest.when instanceof Query) {
      Query query = (Query) this.primedRequest.when;
      ByteBuffer b = ByteBuffer.allocate(4);
      b.putInt(query.getQueryId());
      if (this.primedRequest.then instanceof SuccessResult) {
        SuccessResult result = (SuccessResult) this.primedRequest.then;
        return new Prepared(
            b.array(), null, fetchRowMetadataForParams(query), fetchRowMetadataForResults(result));
      } else if (this.primedRequest.then instanceof VoidResult) {
        return new Prepared(b.array(), null, fetchRowMetadataForParams(query), rowMetadataForVoid);
      } else if (this.primedRequest.then instanceof ErrorResult) {
        return new Prepared(
            b.array(),
            null,
            new RowsMetadata(new LinkedList<ColumnSpec>(), null, null, null),
            new RowsMetadata(new LinkedList<ColumnSpec>(), null, null, null));
      }
    }
    return null;
  }

  List<Action> toPreparedAction(long delayInMs) {
    Prepared preparedResponse = toPrepared();
    MessageResponseAction action = new MessageResponseAction(preparedResponse, delayInMs);
    return Collections.singletonList(action);
  }

  @Override
  public List<Action> getActions(AbstractNode node, Frame frame) {
    if (frame.message instanceof Prepare) {
      if (primedRequest.when instanceof Query) {
        if (primedRequest.then instanceof SuccessResult) {
          // Apply delay if not ignore on prepare.
          long delayInMs =
              !primedRequest.then.isIgnoreOnPrepare() ? primedRequest.then.getDelayInMs() : 0;
          return this.toPreparedAction(delayInMs);
        } else if (primedRequest.then instanceof ErrorResult) {
          // If ignore on prepare, delegate.
          if (primedRequest.then.isIgnoreOnPrepare()) {
            return Collections.emptyList();
          }
        }
      }
    }
    return primedRequest.then.toActions(node, frame);
  }
}
