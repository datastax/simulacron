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
import com.datastax.oss.protocol.internal.response.error.ReadFailure;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.codec.RequestFailureReason;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.util.Map;
import java.util.stream.Collectors;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.READ_FAILURE;

public class ReadFailureResult extends RequestFailureResult {

  @JsonProperty("dataPresent")
  private final boolean dataPresent;

  @JsonCreator
  public ReadFailureResult(
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty(value = "cl", required = true) ConsistencyLevel cl,
      @JsonProperty(value = "received", required = true) int received,
      @JsonProperty(value = "blockFor", required = true) int blockFor,
      @JsonProperty(value = "failureReasonByEndpoint", required = true)
          Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      @JsonProperty(value = "dataPresent", required = true) boolean dataPresent) {
    super(READ_FAILURE, delayInMs, cl, received, blockFor, failureReasonByEndpoint);
    this.dataPresent = dataPresent;
  }

  static Map<InetAddress, Integer> toIntMap(
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
    return failureReasonByEndpoint
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCode()));
  }

  @Override
  public Message toMessage() {
    return new ReadFailure(
        errorMessage,
        cl.getCode(),
        received,
        blockFor,
        failureReasonByEndpoint.size(),
        toIntMap(failureReasonByEndpoint),
        dataPresent);
  }
}
