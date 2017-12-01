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

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.stubbing.Action;
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

  @JsonProperty("ignore_on_prepare")
  protected Boolean ignoreOnPrepare;

  @JsonCreator
  public Result(
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    this.delayInMs = delayInMs;
    this.ignoreOnPrepare = ignoreOnPrepare;
  }

  @JsonIgnore
  public long getDelayInMs() {
    return delayInMs;
  }

  public void setDelay(long delay, TimeUnit delayUnit) {
    this.delayInMs = TimeUnit.MILLISECONDS.convert(delay, delayUnit);
  }

  /**
   * @return Whether or not this result should be applied to a matching prepare statement. Note that
   *     in the case of {@link SuccessResult} this only applies to delay, as we do not want to
   *     return rows responses for prepare messages.
   */
  public boolean isIgnoreOnPrepare() {
    // if not set, return true as that should be the default behavior.
    return ignoreOnPrepare == null ? true : ignoreOnPrepare;
  }

  /**
   * Sets whether or not this result should be applied to a matching prepare statement.
   *
   * @param ignoreOnPrepare Value to set.
   */
  @JsonIgnore
  public void setIgnoreOnPrepare(boolean ignoreOnPrepare) {
    this.ignoreOnPrepare = ignoreOnPrepare;
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
