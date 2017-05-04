package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.SYNTAX_ERROR;

public class SyntaxErrorResult extends ErrorResult {

  SyntaxErrorResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  SyntaxErrorResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(SYNTAX_ERROR, errorMessage, delayInMs);
  }
}
