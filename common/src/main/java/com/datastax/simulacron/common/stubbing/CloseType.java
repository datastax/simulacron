package com.datastax.simulacron.common.stubbing;

import com.fasterxml.jackson.annotation.JsonProperty;

public enum CloseType {
  @JsonProperty("disconnect")
  DISCONNECT,
  @JsonProperty("shutdown_read")
  SHUTDOWN_READ,
  @JsonProperty("shutdown_write")
  SHUTDOWN_WRITE
}
