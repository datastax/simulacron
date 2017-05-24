package com.datastax.simulacron.common.result;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.net.InetAddress;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class RequestFailureResult extends ErrorResult {

  @JsonProperty("consistency_level")
  protected final ConsistencyLevel cl;

  @JsonProperty("received")
  protected final int received;

  @JsonProperty("block_for")
  protected final int blockFor;

  @JsonProperty("failure_reasons")
  protected final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

  protected RequestFailureResult(
      int errorCode,
      ConsistencyLevel cl,
      int received,
      int blockFor,
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      long delayInMs) {
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

  static Map<InetAddress, Integer> toIntMap(
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
    return failureReasonByEndpoint
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCode()));
  }
}
