package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.READ_TIMEOUT;

public class ReadTimeoutResult extends RequestTimeoutResult {

  @JsonProperty("dataPresent")
  private final boolean dataPresent;

  @JsonCreator
  public ReadTimeoutResult(
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty(value = "cl", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "received", required = true) int received,
      @JsonProperty(value = "blockFor", required = true) int blockFor,
      @JsonProperty(value = "dataPresent", required = true) boolean dataPresent) {
    super(READ_TIMEOUT, delayInMs, cl, received, blockFor);
    this.dataPresent = dataPresent;
  }

  @Override
  public Message toMessage() {
    return new ReadTimeout(errorMessage, cl.getCode(), received, blockFor, dataPresent);
  }
}
