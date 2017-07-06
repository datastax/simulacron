package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.InternalStubMapping;
import com.datastax.simulacron.common.stubbing.Prime;

import java.util.List;

/**
 * A stub that wraps another stub and is considered internal. Is useful for registering stubs using
 * the prime api that are meant to be marked internal.
 */
class InternalStubWrapper extends Prime implements InternalStubMapping {

  InternalStubWrapper(Prime wrapped) {
    super(wrapped.getPrimedRequest());
  }

  @Override
  public boolean matches(Frame frame) {
    return super.matches(frame);
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    return super.getActions(node, frame);
  }
}
