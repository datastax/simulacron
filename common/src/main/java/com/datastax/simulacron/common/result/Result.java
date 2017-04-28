package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.Action;
import com.fasterxml.jackson.annotation.*;

import java.util.List;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "result")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ServerErrorResult.class, name = "server_error"),
  @JsonSubTypes.Type(value = ProtocolErrorResult.class, name = "protocol_error"),
  @JsonSubTypes.Type(value = AuthenticationErrorResult.class, name = "authentication_error"),
  @JsonSubTypes.Type(value = AlreadyExistsResult.class, name = "already_exists"),
  @JsonSubTypes.Type(value = SuccessResult.class, name = "success")
})
public abstract class Result {

  @JsonProperty("delay_in_ms")
  protected final long delayInMs;

  @JsonCreator
  public Result(@JsonProperty("delay_in_ms") long delayInMs) {
    this.delayInMs = delayInMs;
  }

  @JsonIgnore
  public long getDelayInMs() {
    return delayInMs;
  }

  public abstract List<Action> toActions(Node node, Frame frame);

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Result result = (Result) o;

    return delayInMs == result.delayInMs;
  }

  @Override
  public int hashCode() {
    return (int) (delayInMs ^ (delayInMs >>> 32));
  }
}
