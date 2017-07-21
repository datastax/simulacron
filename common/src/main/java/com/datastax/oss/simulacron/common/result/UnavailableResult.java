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

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.Unavailable;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.UNAVAILABLE;

public class UnavailableResult extends ErrorResult {

  /** The consistency level of the query that triggered the exception. */
  @JsonProperty("consistency_level")
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

  public UnavailableResult(ConsistencyLevel cl, int required, int alive) {
    this("Cannot achieve consistency level " + cl, cl, required, alive, 0);
  }

  @JsonCreator
  UnavailableResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty(value = "consistency_level", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "required", required = true) int required,
      @JsonProperty(value = "alive", required = true) int alive,
      @JsonProperty("delay_in_ms") long delayInMs) {
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
