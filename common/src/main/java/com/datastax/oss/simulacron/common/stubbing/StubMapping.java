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
package com.datastax.oss.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import java.util.List;

/**
 * Defines a mapping for which given a {@link AbstractNode} and a received {@link Frame} to that
 * node if these two objects meet some criteria, {@link Action}s are produced which should be
 * performed.
 */
public abstract class StubMapping {

  /**
   * Determines whether or not the input {@link AbstractNode} and {@link Frame} meet the criteria.
   *
   * @param node node receiving the frame.
   * @param frame the sent frame.
   * @return whether or not the matching criteria was met.
   */
  public boolean matches(AbstractNode node, Frame frame) {
    return this.matches(frame);
  }

  public abstract boolean matches(Frame frame);

  /**
   * Return the {@link Action}s to perform for the given {@link Frame} received by the {@link
   * AbstractNode}
   *
   * @param node node receiving the frame.
   * @param frame the sent frame.
   * @return the actions to perform as result of receiving the input frame on the input node.
   */
  public abstract List<Action> getActions(AbstractNode node, Frame frame);
}
