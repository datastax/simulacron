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

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.codec.CodecUtils;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

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
      Rows rows = new DefaultRows(buildEmptyRowsMetadata(), peerRows);
      MessageResponseAction action = new MessageResponseAction(rows);
      return Collections.singletonList(action);
    }
    return Collections.emptyList();
  }

  private RowsMetadata buildEmptyRowsMetadata() {
    CodecUtils.ColumnSpecBuilder systemPeers =
        CodecUtils.columnSpecBuilder("whatever_keyspace", "whatever_table");
    List<ColumnSpec> systemPeersSpecs =
        CodecUtils.columnSpecs(systemPeers.apply("key", CodecUtils.primitive(INT)));

    return new RowsMetadata(systemPeersSpecs, null, new int[] {0});
  }
}
