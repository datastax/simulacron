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

import static com.datastax.oss.simulacron.common.stubbing.DisconnectAction.Scope.CONNECTION;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DisconnectAction implements Action {

  public enum Scope {
    @JsonProperty("connection")
    CONNECTION,
    @JsonProperty("node")
    NODE,
    @JsonProperty("data_center")
    DATA_CENTER,
    @JsonProperty("cluster")
    CLUSTER
  }

  private final Scope scope;

  private final long delayInMs;

  private final CloseType closeType;

  DisconnectAction(Scope scope, CloseType closeType, long delayInMs) {
    this.scope = scope;
    this.delayInMs = delayInMs;
    this.closeType = closeType;
  }

  @Override
  public Long delayInMs() {
    return delayInMs;
  }

  public Scope getScope() {
    return scope;
  }

  public CloseType getCloseType() {
    return closeType;
  }

  @Override
  public String toString() {
    return "DisconnectAction{" + "scope=" + scope + ", delayInMs=" + delayInMs + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private Scope scope = CONNECTION;
    private CloseType closeType = CloseType.DISCONNECT;
    long delayInMs = 0L;

    public Builder withScope(Scope scope) {
      this.scope = scope;
      return this;
    }

    public Builder withCloseType(CloseType closeType) {
      this.closeType = closeType;
      return this;
    }

    public Builder withDelayInMs(long delayInMs) {
      this.delayInMs = delayInMs;
      return this;
    }

    public DisconnectAction build() {
      return new DisconnectAction(scope, closeType, delayInMs);
    }
  }
}
