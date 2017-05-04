/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.simulacron.common.result;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.WriteFailure;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.datastax.simulacron.common.codec.WriteType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.util.Map;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.WRITE_FAILURE;
import static com.datastax.simulacron.common.result.ReadFailureResult.toIntMap;

public class WriteFailureResult extends RequestFailureResult {

  @JsonProperty("writeType")
  private final WriteType writeType;

  @JsonCreator
  public WriteFailureResult(
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty(value = "cl", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "received", required = true) int received,
      @JsonProperty(value = "blockFor", required = true) int blockFor,
      @JsonProperty(value = "failureReasonByEndpoint", required = true)
          Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      @JsonProperty(value = "writeType", required = true) WriteType writeType) {
    super(WRITE_FAILURE, delayInMs, cl, received, blockFor, failureReasonByEndpoint);
    this.writeType = writeType;
  }

  @Override
  public Message toMessage() {
    return new WriteFailure(
        errorMessage,
        cl.getCode(),
        received,
        blockFor,
        failureReasonByEndpoint.size(),
        toIntMap(failureReasonByEndpoint),
        writeType.name());
  }
}
