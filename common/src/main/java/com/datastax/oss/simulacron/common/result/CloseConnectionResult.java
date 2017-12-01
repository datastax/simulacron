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
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.DisconnectAction;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;

public class CloseConnectionResult extends Result {

  @JsonProperty("scope")
  private final DisconnectAction.Scope scope;

  @JsonProperty("close_type")
  private final CloseType closeType;

  public CloseConnectionResult(DisconnectAction.Scope scope, CloseType closeType) {
    this(scope, closeType, 0, null);
  }

  @JsonCreator
  public CloseConnectionResult(
      @JsonProperty("scope") DisconnectAction.Scope scope,
      @JsonProperty("close_type") CloseType closeType,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    super(delayInMs, ignoreOnPrepare);
    this.scope = scope;
    this.closeType = closeType;
  }

  @Override
  public List<Action> toActions(AbstractNode node, Frame frame) {
    DisconnectAction.Builder builder = DisconnectAction.builder().withDelayInMs(delayInMs);
    if (scope != null) {
      builder.withScope(scope);
    }
    if (closeType != null) {
      builder.withCloseType(closeType);
    }
    return Collections.singletonList(builder.build());
  }
}
