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
package com.datastax.oss.simulacron.common.stubbing;

import static com.datastax.oss.simulacron.common.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.util.List;
import org.junit.Test;

public class EmptyReturnMetadataHandlerTest {

  private static String[] sampleStrings = {
    "SELECT * FROM system_schema.keyspaces",
    "SELECT * FROM system_schema.views",
    "SELECT * FROM system_schema.tables",
    "SELECT * FROM system_schema.columns",
    "SELECT * FROM system_schema.indexes",
    "SELECT * FROM system_schema.triggers",
    "SELECT * FROM system_schema.types",
    "SELECT * FROM system_schema.functions",
    "Whatever"
  };

  private EmptyReturnMetadataHandler handler =
      new EmptyReturnMetadataHandler("SELECT * FROM system_schema.keyspaces");

  private static Frame queryFrame(String queryString) {
    return FrameUtils.wrapRequest(new Query(queryString));
  }

  @Test
  public void shouldReturnAction() {
    // Should returned the one single expected message
    for (String s : sampleStrings) {
      List<Action> nodeActions = new EmptyReturnMetadataHandler(s).getActions(null, queryFrame(s));

      assertThat(nodeActions).hasSize(1);

      Action nodeAction = nodeActions.get(0);
      assertThat(nodeAction).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) nodeAction).getMessage();

      assertThat(node0Message).isRows().hasRows(0);
    }
  }

  @Test
  public void shouldMatch() {
    // Should match the following queries.
    for (String s : sampleStrings) {
      assertThat(new EmptyReturnMetadataHandler(s).matches(null, queryFrame(s))).isTrue();
    }
  }

  @Test
  public void shouldNotMatch() {
    // Should not match queries that aren't peer related.
    assertThat(handler.matches(null, queryFrame("SELECT foo FROM bar"))).isFalse();
    // Should not match non-queries
    assertThat(handler.matches(null, FrameUtils.wrapRequest(new Startup()))).isFalse();
    assertThat(handler.matches(null, FrameUtils.wrapRequest(Options.INSTANCE))).isFalse();
  }

  @Test
  public void shouldReturnNoActionsForNonMatchingQuery() {
    // Should not return any actions if the query doesn't match.
    assertThat(handler.getActions(null, FrameUtils.wrapRequest(new Startup()))).isEmpty();
  }
}
