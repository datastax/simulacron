package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class StubStore {

  private final CopyOnWriteArrayList<StubMapping> stubMappings = new CopyOnWriteArrayList<>();

  public StubStore() {}

  public void register(StubMapping mapping) {
    stubMappings.add(mapping);
  }

  public void registerInternal(Prime prime) {
    stubMappings.add(new InternalStubWrapper(prime));
  }

  public int clear() {
    int size = stubMappings.size();
    stubMappings.clear();
    return size;
  }

  public Optional<StubMapping> find(BoundNode node, Frame frame) {
    return stubMappings.stream().filter(s -> s.matches(node, frame)).findFirst();
  }
}
