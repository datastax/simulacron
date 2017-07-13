package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.NoResponseAction;

import java.util.Collections;
import java.util.List;

public class NoResult extends Result {

  public NoResult() {
    super(0);
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    return Collections.singletonList(new NoResponseAction());
  }
}
