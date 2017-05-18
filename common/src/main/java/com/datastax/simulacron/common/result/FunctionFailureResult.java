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

  @JsonProperty("function")
  private final String name;

  @JsonProperty("arg_types")
  private final List<String> argTypes;

  public FunctionFailureResult(
      String keyspace, String function, List<String> argTypes, String detail) {
    this(keyspace, function, argTypes, detail, 0);
  }

  @JsonCreator
  public FunctionFailureResult(
      @JsonProperty("keyspace") String keyspace,
      @JsonProperty(value = "function", required = true) String function,
      @JsonProperty("arg_types") List<String> argTypes,
      @JsonProperty("detail") String detail,
      @JsonProperty("delay_in_ms") long delayInMs) {
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
