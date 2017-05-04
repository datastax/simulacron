package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.CONFIG_ERROR;

public class ConfigurationErrorResult extends ErrorResult {

  ConfigurationErrorResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  ConfigurationErrorResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(CONFIG_ERROR, errorMessage, delayInMs);
  }
}
