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
import com.datastax.oss.protocol.internal.response.error.ReadTimeout;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.READ_TIMEOUT;

public class ReadTimeoutResult extends RequestTimeoutResult {

  @JsonProperty("data_present")
  private final boolean dataPresent;

  public ReadTimeoutResult(ConsistencyLevel cl, int received, int blockFor, boolean dataPresent) {
    this(cl, received, blockFor, dataPresent, 0);
  }

  @JsonCreator
  public ReadTimeoutResult(
      @JsonProperty(value = "consistency_level", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "received", required = true) int received,
      @JsonProperty(value = "block_for", required = true) int blockFor,
      @JsonProperty(value = "data_present", required = true) boolean dataPresent,
      @JsonProperty("delay_in_ms") long delayInMs) {
    super(READ_TIMEOUT, cl, received, blockFor, delayInMs);
    this.dataPresent = dataPresent;
  }

  @Override
  public Message toMessage() {
    return new ReadTimeout(errorMessage, cl.getCode(), received, blockFor, dataPresent);
  }
}
