package com.datastax.simulacron.common.result;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class RequestTimeoutResult extends ErrorResult {

  @JsonProperty("cl")
  protected final ConsistencyLevel cl;

  @JsonProperty("received")
  protected final int received;

  @JsonProperty("blockFor")
  protected final int blockFor;

  protected RequestTimeoutResult(
      int errorCode, long delayInMs, ConsistencyLevel cl, int received, int blockFor) {
    super(
        errorCode,
        String.format("Operation timed out - received only %d responses.", received),
        delayInMs);
    this.cl = cl;
    this.received = received;
    this.blockFor = blockFor;
  }
}
