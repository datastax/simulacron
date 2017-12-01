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

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.WRITE_TIMEOUT;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.WriteTimeout;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class WriteTimeoutResult extends RequestTimeoutResult {

  @JsonProperty("write_type")
  private final WriteType writeType;

  public WriteTimeoutResult(ConsistencyLevel cl, int received, int blockFor, WriteType writeType) {
    this(cl, received, blockFor, writeType, 0, null);
  }

  @JsonCreator
  public WriteTimeoutResult(
      @JsonProperty(value = "consistency_level", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "received", required = true) int received,
      @JsonProperty(value = "block_for", required = true) int blockFor,
      @JsonProperty(value = "write_type", required = true) WriteType writeType,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    super(WRITE_TIMEOUT, cl, received, blockFor, delayInMs, ignoreOnPrepare);
    this.writeType = writeType;
  }

  @Override
  public Message toMessage() {
    return new WriteTimeout(errorMessage, cl.getCode(), received, blockFor, writeType.name());
  }
}
