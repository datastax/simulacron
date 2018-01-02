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

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.ALREADY_EXISTS;

import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.response.error.AlreadyExists;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class AlreadyExistsResult extends ErrorResult {

  @JsonProperty("keyspace")
  private final String keyspace;

  @JsonProperty("table")
  private final String table;

  public AlreadyExistsResult(String errorMessage, String keyspace, String table) {
    this(errorMessage, keyspace, table, 0, null);
  }

  public AlreadyExistsResult(String errorMessage, String keyspace) {
    this(errorMessage, keyspace, null, 0, null);
  }

  @JsonCreator
  public AlreadyExistsResult(
      @JsonProperty("message") String errorMessage,
      @JsonProperty(value = "keyspace", required = true) String keyspace,
      @JsonProperty(value = "table") String table,
      @JsonProperty("delayInMs") long delayInMs,
      @JsonProperty("ignore_on_prepare") Boolean ignoreOnPrepare) {
    super(ALREADY_EXISTS, errorMessage, delayInMs, ignoreOnPrepare);
    this.keyspace = keyspace;
    this.table = table;
  }

  @Override
  public Message toMessage() {
    return new AlreadyExists(errorMessage, keyspace, table != null ? table : "");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    AlreadyExistsResult that = (AlreadyExistsResult) o;
    return Objects.equals(keyspace, that.keyspace) && Objects.equals(table, that.table);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), keyspace, table);
  }
}
