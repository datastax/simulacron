/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.result;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.FUNCTION_FAILURE;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.FunctionFailure;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class FunctionFailureResult extends ErrorResult {

  @JsonProperty("keyspace")
  private final String keyspace;

  @JsonProperty("function")
  private final String name;

  @JsonProperty("arg_types")
  private final List<String> argTypes;

  public FunctionFailureResult(
      String keyspace, String function, List<String> argTypes, String detail) {
    this(keyspace, function, argTypes, detail, 0, false);
  }

  @JsonCreator
  public FunctionFailureResult(
      @JsonProperty("keyspace") String keyspace,
      @JsonProperty(value = "function", required = true) String function,
      @JsonProperty("arg_types") List<String> argTypes,
      @JsonProperty("detail") String detail,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") boolean ignoreOnPrepare) {
    super(
        FUNCTION_FAILURE,
        "execution of '" + functionName(keyspace, function) + argTypes + "' failed: " + detail,
        delayInMs,
        ignoreOnPrepare);
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
