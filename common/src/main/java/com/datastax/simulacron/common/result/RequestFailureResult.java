package com.datastax.simulacron.common.result;

import java.net.InetAddress;
import java.util.Map;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.fasterxml.jackson.annotation.JsonProperty;

public abstract class RequestFailureResult extends ErrorResult {

  @JsonProperty("cl")
  protected final ConsistencyLevel cl;

  @JsonProperty("received")
  protected final int received;

  @JsonProperty("blockFor")
  protected final int blockFor;

  @JsonProperty("failureReasonByEndpoint")
  protected final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

  protected RequestFailureResult(
      int errorCode,
      long delayInMs,
      ConsistencyLevel cl,
      int received,
      int blockFor,
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
    super(
        errorCode,
        String.format(
            "Operation failed - received %d responses and %d failures",
            received, failureReasonByEndpoint.size()),
        delayInMs);
    this.cl = cl;
    this.received = received;
    this.blockFor = blockFor;
    this.failureReasonByEndpoint = failureReasonByEndpoint;
  }
}
