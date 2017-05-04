package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.UNAUTHORIZED;

public class UnauthorizedResult extends ErrorResult {

  UnauthorizedResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  UnauthorizedResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(UNAUTHORIZED, errorMessage, delayInMs);
  }
}
