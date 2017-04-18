package com.datastax.simulacron.common.stubbing;

public class DisconnectAction implements Action {

  public enum Scope {
    CONNECTION,
    NODE,
    DATACENTER,
    CLUSTER
  }

  private final Scope scope;

  private final long delayInMs;

  public DisconnectAction() {
    this(Scope.CONNECTION, 0L);
  }

  public DisconnectAction(Scope scope) {
    this(scope, 0L);
  }

  public DisconnectAction(Scope scope, long delayInMs) {
    this.scope = scope;
    this.delayInMs = delayInMs;
  }

  @Override
  public Long delayInMs() {
    return delayInMs;
  }

  public Scope getScope() {
    return scope;
  }

  @Override
  public String toString() {
    return "DisconnectAction{" + "scope=" + scope + ", delayInMs=" + delayInMs + '}';
  }
}
