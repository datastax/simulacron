package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.OVERLOADED;

public class OverloadedResult extends ErrorResult {

  public OverloadedResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  public OverloadedResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(OVERLOADED, errorMessage, delayInMs);
  }
}
