/*
 * Copyright DataStax, Inc.
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
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.stubbing.Action;
import com.datastax.oss.simulacron.common.stubbing.InternalStubMapping;
import com.datastax.oss.simulacron.common.stubbing.Prime;
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
  public List<Action> getActions(AbstractNode node, Frame frame) {
    return super.getActions(node, frame);
  }
}
