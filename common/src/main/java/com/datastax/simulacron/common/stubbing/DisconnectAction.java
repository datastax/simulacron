package com.datastax.simulacron.common.stubbing;

import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.simulacron.common.stubbing.DisconnectAction.Scope.CONNECTION;

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
