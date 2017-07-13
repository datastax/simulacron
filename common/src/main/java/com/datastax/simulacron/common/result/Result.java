package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.AbstractNode;
import com.datastax.simulacron.common.stubbing.Action;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;
import java.util.concurrent.TimeUnit;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "result")
@JsonSubTypes({
  @JsonSubTypes.Type(value = SuccessResult.class, name = "success"),
  @JsonSubTypes.Type(value = NoResult.class, name = "no_result"),
  @JsonSubTypes.Type(value = ServerErrorResult.class, name = "server_error"),
  @JsonSubTypes.Type(value = ProtocolErrorResult.class, name = "protocol_error"),
  @JsonSubTypes.Type(value = AuthenticationErrorResult.class, name = "authentication_error"),
  @JsonSubTypes.Type(value = UnavailableResult.class, name = "unavailable"),
  @JsonSubTypes.Type(value = AlreadyExistsResult.class, name = "already_exists"),
  @JsonSubTypes.Type(value = ConfigurationErrorResult.class, name = "config_error"),
  @JsonSubTypes.Type(value = FunctionFailureResult.class, name = "function_failure"),
  @JsonSubTypes.Type(value = InvalidResult.class, name = "invalid"),
  @JsonSubTypes.Type(value = IsBootstrappingResult.class, name = "is_bootstrapping"),
  @JsonSubTypes.Type(value = OverloadedResult.class, name = "overloaded"),
  @JsonSubTypes.Type(value = ReadFailureResult.class, name = "read_failure"),
  @JsonSubTypes.Type(value = ReadTimeoutResult.class, name = "read_timeout"),
  @JsonSubTypes.Type(value = SyntaxErrorResult.class, name = "syntax_error"),
  @JsonSubTypes.Type(value = TruncateErrorResult.class, name = "truncate_error"),
  @JsonSubTypes.Type(value = UnauthorizedResult.class, name = "unauthorized"),
  @JsonSubTypes.Type(value = UnpreparedResult.class, name = "unprepared"),
  @JsonSubTypes.Type(value = WriteFailureResult.class, name = "write_failure"),
  @JsonSubTypes.Type(value = WriteTimeoutResult.class, name = "write_timeout"),
  @JsonSubTypes.Type(value = CloseConnectionResult.class, name = "close_connection"),
  @JsonSubTypes.Type(value = VoidResult.class, name = "void")
})
public abstract class Result {

  @JsonProperty("delay_in_ms")
  protected long delayInMs;

  @JsonCreator
  public Result(@JsonProperty("delay_in_ms") long delayInMs) {
    this.delayInMs = delayInMs;
  }

  @JsonIgnore
  public long getDelayInMs() {
    return delayInMs;
  }

  public void setDelay(long delay, TimeUnit delayUnit) {
    this.delayInMs = TimeUnit.MILLISECONDS.convert(delay, delayUnit);
  }

  public abstract List<Action> toActions(AbstractNode node, Frame frame);

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
