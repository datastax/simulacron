/*
 * Copyright (C) 2017-2017 DataStax Inc.
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
package com.datastax.oss.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
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
