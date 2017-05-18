package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.SERVER_ERROR;

public class ServerErrorResult extends ErrorResult {

  public ServerErrorResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  public ServerErrorResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(SERVER_ERROR, errorMessage, delayInMs);
  }
}
