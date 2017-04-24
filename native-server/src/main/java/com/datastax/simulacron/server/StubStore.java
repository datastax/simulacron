package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

public class StubStore {

  private final CopyOnWriteArrayList<StubMapping> stubMappings = new CopyOnWriteArrayList<>();

  StubStore() {}

  public void register(StubMapping mapping) {
    stubMappings.add(mapping);
  }

  public void clearAllMatchingType(Class clazz) {
    Iterator<StubMapping> iterator = stubMappings.iterator();
    while (iterator.hasNext()) {
      StubMapping mapping = iterator.next();
      if (mapping.getClass().equals(clazz)) {
        stubMappings.remove(mapping);
      }
    }
  }

  public void clear() {
    stubMappings.clear();
  }

  public List<Action> handle(Node node, Frame frame) {
    Optional<StubMapping> stubMapping = find(node, frame);
    if (stubMapping.isPresent()) {
      return stubMapping.get().getActions(node, frame);
    } else {
      return Collections.emptyList();
    }
  }

  Optional<StubMapping> find(Node node, Frame frame) {
    return stubMappings.stream().filter(s -> s.matches(node, frame)).findFirst();
  }
}
