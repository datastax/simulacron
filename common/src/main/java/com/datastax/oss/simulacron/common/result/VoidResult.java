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
package com.datastax.oss.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.stubbing.Action;
import com.datastax.oss.simulacron.common.stubbing.MessageResponseAction;
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
