/*
 * Copyright DataStax, Inc.
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

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.CONFIG_ERROR;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class ConfigurationErrorResult extends ErrorResult {

  public ConfigurationErrorResult(String errorMessage) {
    this(errorMessage, 0, null);
  }

  @JsonCreator
  public ConfigurationErrorResult(
      @JsonProperty(value = "message", required = true) String errorMessage,
      @JsonProperty("delay_in_ms") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    super(CONFIG_ERROR, errorMessage, delayInMs, ignoreOnPrepare);
  }
}
