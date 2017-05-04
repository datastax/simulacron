package com.datastax.simulacron.common.result;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.IS_BOOTSTRAPPING;

public class IsBoostrappingResult extends ErrorResult {

  IsBoostrappingResult() {
    this(0);
  }

  @JsonCreator
  IsBoostrappingResult(@JsonProperty("delay_in_ms") long delayInMs) {
    super(IS_BOOTSTRAPPING, "Cannot read from a bootstrapping node", delayInMs);
  }
}
