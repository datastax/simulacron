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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.UNPREPARED;

public class UnpreparedResult extends ErrorResult {

  public UnpreparedResult(String errorMessage) {
    this(errorMessage, 0);
  }

  @JsonCreator
  public UnpreparedResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(UNPREPARED, errorMessage, delayInMs);
  }
}
