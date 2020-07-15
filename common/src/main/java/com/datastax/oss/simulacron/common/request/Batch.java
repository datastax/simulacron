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
package com.datastax.oss.simulacron.common.request;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public final class Batch extends Request {
  @JsonProperty("queries")
  public final List<Statement> queries;

  public final transient List<ConsistencyLevel> consistencyEnum;

  @JsonCreator
  public Batch(
      @JsonProperty("queries") List<Statement> queries,
      @JsonProperty("consistency_level") String[] consistency) {
    this(queries, createEnumFromConsistency(consistency));
  }

  public Batch(List<Statement> queries, List<ConsistencyLevel> consistencies) {
    this.queries = queries;
    this.consistencyEnum = consistencies;
  }

  @JsonProperty("queries")
  public List<Statement> getQueries() {
    return queries;
  }

  public boolean matches(Frame frame) {

    if (frame.message instanceof com.datastax.oss.protocol.internal.request.Batch) {
      com.datastax.oss.protocol.internal.request.Batch batch =
          (com.datastax.oss.protocol.internal.request.Batch) frame.message;

      ConsistencyLevel level = ConsistencyLevel.fromCode(batch.consistency);
      // NOTE: Absent CL level means it will match all CL levels
      if (this.consistencyEnum.contains(level) || this.consistencyEnum.size() == 0) {

        if (batch.values.size() != queries.size()) {
          return false;
        }

        Iterator<List<ByteBuffer>> valuesIterator = batch.values.iterator();
        Iterator<Statement> statementIterator = queries.iterator();
        Iterator<Object> queriesIterator = batch.queriesOrIds.iterator();

        while (valuesIterator.hasNext()) {
          if (!statementIterator
              .next()
              .checkStatementMatch(
                  frame.protocolVersion, queriesIterator.next(), valuesIterator.next())) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Batch batch = (Batch) o;

    if (!queries.equals(batch.queries)) return false;
    return consistencyEnum.equals(batch.consistencyEnum);
  }

  @Override
  public int hashCode() {
    int result = queries.hashCode();
    result = 31 * result + consistencyEnum.hashCode();
    return result;
  }
}
