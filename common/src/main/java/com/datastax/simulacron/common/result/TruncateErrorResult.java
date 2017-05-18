package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.TRUNCATE_ERROR;

public class TruncateErrorResult extends ErrorResult {

  public TruncateErrorResult(String message) {
    this(message, 0);
  }

  @JsonCreator
  public TruncateErrorResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(TRUNCATE_ERROR, errorMessage, delayInMs);
  }
}
