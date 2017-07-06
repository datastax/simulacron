package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.util.Iterator;
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

  public int clear(Class clazz) {
    Iterator<StubMapping> iterator = stubMappings.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      StubMapping mapping = iterator.next();
      if (mapping.getClass().equals(clazz)) {
        stubMappings.remove(mapping);
        count++;
      }
    }
    return count;
  }

  public void clear() {
    stubMappings.clear();
  }

  public Optional<StubMapping> find(Node node, Frame frame) {
    return stubMappings.stream().filter(s -> s.matches(node, frame)).findFirst();
  }
}
