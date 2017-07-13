package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.CloseType;
import com.datastax.simulacron.common.stubbing.DisconnectAction;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class CloseConnectionResult extends Result {

  @JsonProperty("scope")
  private final DisconnectAction.Scope scope;

  @JsonProperty("close_type")
  private final CloseType closeType;

  public CloseConnectionResult(
      @JsonProperty("scope") DisconnectAction.Scope scope,
      @JsonProperty("close_type") CloseType closeType,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(delayInMs);
    this.scope = scope;
    this.closeType = closeType;
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    DisconnectAction.Builder builder = DisconnectAction.builder().withDelayInMs(delayInMs);
    if (scope != null) {
      builder.withScope(scope);
    }
    if (closeType != null) {
      builder.withCloseType(closeType);
    }
    return Collections.singletonList(builder.build());
  }
}
