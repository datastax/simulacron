package com.datastax.simulacron.http.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.RequestPrime;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.util.List;

public class RequestHandler extends StubMapping {
  RequestPrime primedRequest;

  public RequestHandler(RequestPrime primedQuery) {
    this.primedRequest = primedQuery;
  }

  @Override
  public boolean matches(Frame frame) {
    return primedRequest.when.matches(frame);
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    return primedRequest.then.toActions(node, frame);
  }
}
