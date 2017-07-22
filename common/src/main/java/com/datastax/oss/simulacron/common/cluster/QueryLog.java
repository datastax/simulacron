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
package com.datastax.oss.simulacron.common.cluster;

import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.SocketAddress;

public class QueryLog {

  @JsonProperty("query")
  private String query;

  @JsonProperty("consistency_level")
  private ConsistencyLevel consistency;

  @JsonProperty("serial_consistency_level")
  private ConsistencyLevel serialConsistency;

  @JsonProperty("connection")
  private SocketAddress connection;

  @JsonProperty("timestamp")
  private long timestamp;

  @JsonProperty("primed")
  private boolean primed;

  @JsonCreator
  public QueryLog(
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") ConsistencyLevel consistency,
      @JsonProperty("serial_consistency_level") ConsistencyLevel serialConsistency,
      @JsonProperty("connection") SocketAddress connection,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("primed") boolean primed) {
    this.query = query;
    this.consistency = consistency;
    this.serialConsistency = serialConsistency;
    this.connection = connection;
    this.timestamp = timestamp;
    this.primed = primed;
  }

  public String getQuery() {
    return query;
  }

  public ConsistencyLevel getConsistency() {
    return consistency;
  }

  public ConsistencyLevel getSerialConsistency() {
    return serialConsistency;
  }

  public SocketAddress getConnection() {
    return connection;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isPrimed() {
    return primed;
  }
}
