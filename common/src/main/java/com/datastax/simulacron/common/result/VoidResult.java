package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.MessageResponseAction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class VoidResult extends Result {

  private List<Action> actions;

  public VoidResult() {
    this(0);
  }

  @JsonCreator
  public VoidResult(@JsonProperty("delay_in_ms") long delayInMs) {
    super(delayInMs);
    updateActions();
  }

  private void updateActions() {
    this.actions =
        Collections.singletonList(
            new MessageResponseAction(
                com.datastax.oss.protocol.internal.response.result.Void.INSTANCE, delayInMs));
  }

  @Override
  public void setDelay(long delay, TimeUnit delayUnit) {
    super.setDelay(delay, delayUnit);
    updateActions();
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    return this.actions;
  }
}
