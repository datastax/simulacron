package com.datastax.simulacron.common.stubbing;

public class CloseConnectionAction implements Action {

  private final long delayInMs;

  public CloseConnectionAction() {
    this(0L);
  }

  public CloseConnectionAction(long delayInMs) {
    this.delayInMs = delayInMs;
  }

  @Override
  public Long delayInMs() {
    return delayInMs;
  }
}
