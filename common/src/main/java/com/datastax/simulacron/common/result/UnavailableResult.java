package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.UNAVAILABLE;

public class UnavailableResult extends ErrorResult {

  /** The consistency level of the query that triggered the exception. */
  @JsonProperty("cl")
  private final ConsistencyLevel cl;

  /** The number of nodes that should be alive to respect <cl>. */
  @JsonProperty("required")
  private final int required;

  /**
   * The number of replicas that were known to be alive when the request had been processed (since
   * an unavailable exception has been triggered, there will be <alive> < <required>)
   */
  @JsonProperty("alive")
  private final int alive;

  UnavailableResult(long delayInMs, ConsistencyLevel cl, int required, int alive) {
    this("Cannot achieve consistency level " + cl, delayInMs, cl, required, alive);
  }

  UnavailableResult(long delayInMs, ConsistencyLevel cl, String dc, int required, int alive) {
    this("Cannot achieve consistency level " + cl + " in DC " + dc, delayInMs, cl, required, alive);
  }

  @JsonCreator
  UnavailableResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty(value = "cl", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "required", required = true) int required,
      @JsonProperty(value = "alive", required = true) int alive) {
    super(UNAVAILABLE, errorMessage, delayInMs);
    this.cl = cl;
    this.required = required;
    this.alive = alive;
  }

  @Override
  public Message toMessage() {
    return new Unavailable(errorMessage, cl.getCode(), required, alive);
  }
}
