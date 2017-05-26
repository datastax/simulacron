package com.datastax.simulacron.common.stubbing;

/* This class is necessary because the default response when a query is not found
is to return an empty list, this action means nothing is returned */
public class NoResponseAction implements Action {
  public NoResponseAction() {}

  @Override
  public Long delayInMs() {
    return (long) 0;
  }
}
