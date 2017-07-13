package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.simulacron.common.utils.FrameUtils;
import org.junit.Test;

import java.util.List;

import static com.datastax.simulacron.common.Assertions.assertThat;

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
