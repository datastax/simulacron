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
package com.datastax.oss.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.SocketAddress;
import java.util.Optional;

public class QueryLog {

  @JsonProperty("type")
  private String type;

  @JsonProperty("query")
  private String query;

  @JsonProperty("consistency_level")
  private ConsistencyLevel consistency;

  @JsonProperty("serial_consistency_level")
  private ConsistencyLevel serialConsistency;

  @JsonProperty("connection")
  private SocketAddress connection;

  @JsonProperty("received_timestamp")
  private long receivedTimestamp;

  @JsonProperty("client_timestamp")
  private long clientTimestamp;

  @JsonProperty("primed")
  private boolean primed;

  @JsonProperty(value = "frame", access = JsonProperty.Access.READ_ONLY)
  private Frame frame;

  @JsonCreator
  public QueryLog(
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") ConsistencyLevel consistency,
      @JsonProperty("serial_consistency_level") ConsistencyLevel serialConsistency,
      @JsonProperty("connection") SocketAddress connection,
      @JsonProperty("received_timestamp") long receivedTimestamp,
      @JsonProperty("client_timestamp") long clientTimestamp,
      @JsonProperty("primed") boolean primed) {
    this.query = query;
    this.consistency = consistency;
    this.serialConsistency = serialConsistency;
    this.connection = connection;
    this.clientTimestamp = clientTimestamp;
    this.receivedTimestamp = receivedTimestamp;
    this.primed = primed;
  }

  QueryLog(
      Frame frame,
      SocketAddress connection,
      long receivedTimestamp,
      boolean primed,
      Optional<StubMapping> stubOption) {
    this.frame = frame;
    this.connection = connection;
    this.receivedTimestamp = receivedTimestamp;
    this.primed = primed;
    this.type = frame.message.getClass().getSimpleName().toUpperCase();

    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      this.query = query.query;
      this.consistency = ConsistencyLevel.fromCode(query.options.consistency);
      this.serialConsistency = ConsistencyLevel.fromCode(query.options.serialConsistency);
      this.clientTimestamp = query.options.defaultTimestamp;
    } else if (frame.message instanceof Execute) {
      Execute execute = (Execute) frame.message;
      this.consistency = ConsistencyLevel.fromCode(execute.options.consistency);
      this.serialConsistency = ConsistencyLevel.fromCode(execute.options.serialConsistency);
      this.clientTimestamp = execute.options.defaultTimestamp;
      if (stubOption.isPresent()) {
        StubMapping stub = stubOption.get();
        if (stub instanceof Prime) {
          Prime prime = (Prime) stub;
          if (prime.getPrimedRequest().when
              instanceof com.datastax.oss.simulacron.common.request.Query) {
            com.datastax.oss.simulacron.common.request.Query query =
                (com.datastax.oss.simulacron.common.request.Query) prime.getPrimedRequest().when;
            this.query = query.query;
          }
        }
      }
    } else if (frame.message instanceof Prepare) {
      Prepare prepare = (Prepare) frame.message;
      this.query = prepare.cqlQuery;
    } else if (frame.message instanceof Batch) {
      Batch batch = (Batch) frame.message;
      this.clientTimestamp = batch.defaultTimestamp;
    } else {
      // in the case where we don't know how to extract info from the message, just set the query to
      // the type of message.
      this.query = frame.message.getClass().getSimpleName().toUpperCase();
    }
  }

  /** @deprecated Use frame instead. */
  public String getType() {
    return type;
  }

  /** @deprecated Use frame instead. */
  public String getQuery() {
    return query;
  }

  /** @deprecated Use frame instead. */
  public ConsistencyLevel getConsistency() {
    return consistency;
  }

  /** @deprecated Use frame instead. */
  public ConsistencyLevel getSerialConsistency() {
    return serialConsistency;
  }

  public SocketAddress getConnection() {
    return connection;
  }

  public long getReceivedTimestamp() {
    return receivedTimestamp;
  }

  /** @deprecated Use frame instead. */
  @Deprecated
  public long getClientTimestamp() {
    return clientTimestamp;
  }

  public boolean isPrimed() {
    return primed;
  }

  /** @return The frame associated with this log if present. */
  public Frame getFrame() {
    return this.frame;
  }

  @Override
  public String toString() {
    return "QueryLog{"
        + "type='"
        + type
        + '\''
        + ", query='"
        + query
        + '\''
        + ", consistency="
        + consistency
        + ", serialConsistency="
        + serialConsistency
        + ", connection="
        + connection
        + ", received_timestamp="
        + receivedTimestamp
        + ", client_timestamp="
        + clientTimestamp
        + ", primed="
        + primed
        + '}';
  }
}
