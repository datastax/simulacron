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

import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.RequestFailureReason;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class RequestFailureResult extends ErrorResult {

  @JsonProperty("consistency_level")
  protected final ConsistencyLevel cl;

  @JsonProperty("received")
  protected final int received;

  @JsonProperty("block_for")
  protected final int blockFor;

  @JsonProperty("failure_reasons")
  protected final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

  @JsonProperty("ignore_on_prepare")
  boolean ignoreOnPrepare;

  protected RequestFailureResult(
      int errorCode,
      ConsistencyLevel cl,
      int received,
      int blockFor,
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint,
      long delayInMs,
      boolean ignoreOnPrepare) {
    super(
        errorCode,
        String.format(
            "Operation failed - received %d responses and %d failures",
            received, failureReasonByEndpoint.size()),
        delayInMs,
        ignoreOnPrepare);
    this.cl = cl;
    this.received = received;
    this.blockFor = blockFor;
    this.failureReasonByEndpoint = failureReasonByEndpoint;
  }

  static Map<InetAddress, Integer> toIntMap(
      Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
    return failureReasonByEndpoint
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getCode()));
  }
}
