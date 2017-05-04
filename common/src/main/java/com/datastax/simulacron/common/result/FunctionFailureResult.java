package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.FunctionFailure;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.FUNCTION_FAILURE;

public class FunctionFailureResult extends ErrorResult {

  @JsonProperty("keyspace")
  private final String keyspace;

  @JsonProperty("name")
  private final String name;

  @JsonProperty("argTypes")
  private final List<String> argTypes;

  @JsonCreator
  public FunctionFailureResult(
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("keyspace") String keyspace,
      @JsonProperty(value = "function", required = true) String function,
      @JsonProperty("argTypes") List<String> argTypes,
      @JsonProperty("detail") String detail) {
    super(
        FUNCTION_FAILURE,
        "execution of '" + functionName(keyspace, function) + argTypes + "' failed: " + detail,
        delayInMs);
    this.keyspace = keyspace;
    this.name = function;
    this.argTypes = argTypes;
  }

  private static String functionName(String keyspace, String name) {
    return keyspace == null ? name : keyspace + "." + name;
  }

  @Override
  public Message toMessage() {
    return new FunctionFailure(errorMessage, keyspace, name, argTypes);
  }
}
