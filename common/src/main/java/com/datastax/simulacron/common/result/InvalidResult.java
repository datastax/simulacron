package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.INVALID;

public class InvalidResult extends ErrorResult {

  InvalidResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  InvalidResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(INVALID, errorMessage, delayInMs);
  }
}
