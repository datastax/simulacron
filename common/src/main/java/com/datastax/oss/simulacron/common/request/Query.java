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

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_EMPTY;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.CqlMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class Query extends Request {
  public final String query;
  public final transient List<ConsistencyLevel> consistencyEnum;

  @JsonProperty("param_types")
  @JsonInclude(NON_EMPTY)
  public final Map<String, String> paramTypes;

  @JsonInclude(NON_EMPTY)
  public final Map<String, Object> params;

  public Query(String query) {
    this(query, Collections.emptyList(), null, null);
  }

  @JsonCreator
  public Query(
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") String[] consistency,
      @JsonProperty("params") LinkedHashMap<String, Object> params,
      @JsonProperty("param_types") LinkedHashMap<String, String> paramTypes) {
    this(query, createEnumFromConsistency(consistency), params, paramTypes);
  }

  public Query(
      String query,
      List<ConsistencyLevel> consistencies,
      LinkedHashMap<String, Object> params,
      LinkedHashMap<String, String> paramTypes) {
    this.query = query;
    this.consistencyEnum = consistencies;
    this.params = params;
    this.paramTypes = paramTypes;
  }

  @JsonProperty("consistency_level")
  @JsonInclude(NON_EMPTY)
  public String[] getConsistency() {
    String[] consistency = new String[consistencyEnum.size()];
    for (int i = 0; i < consistencyEnum.size(); i++) {
      consistency[i] = consistencyEnum.get(i).toString();
    }
    return consistency;
  }

  public boolean matches(Frame frame) {

    if (frame.message instanceof Prepare) {
      Prepare prepare = (Prepare) frame.message;
      if (query.equals(prepare.cqlQuery)) {
        return true;
      }
    }
    if (frame.message instanceof Execute) {
      Execute execute = (Execute) frame.message;
      Integer queryIdInt = new BigInteger(execute.queryId).intValue();
      if (getQueryId() == queryIdInt) {
        return checkParamsMatch(execute.options, frame);
      }
    }
    if (frame.message instanceof com.datastax.oss.protocol.internal.request.Query) {
      // If the query expected matches the primed query example and CL levels match. Return true;
      com.datastax.oss.protocol.internal.request.Query query =
          (com.datastax.oss.protocol.internal.request.Query) frame.message;
      if (this.query.equals(query.query)) {
        ConsistencyLevel level = ConsistencyLevel.fromCode(query.options.consistency);
        // NOTE: Absent CL level means it will match all CL levels
        if (this.consistencyEnum.contains(level) || this.consistencyEnum.size() == 0) {
          // Id any parameters are present we must make sure they match those in the primed queries
          return checkParamsMatch(query.options, frame);
        }
      }
    }
    return false;
  }

  /**
   * * A method that will check to see if primed query and actual query parameters match
   *
   * @param options query options to check.
   * @param frame The incoming frame.
   * @return True if the parameters match;
   */
  private boolean checkParamsMatch(QueryOptions options, Frame frame) {
    // I don't like this, but I can't see a way to simplify it.

    // No params match criteria was specified, simply return true.
    if (params == null || params.size() == 0) {
      return true;
    }

    if (options.namedValues.size() != params.size()
        && options.positionalValues.size() != params.size()) {
      return false;
    }
    if ((options.namedValues.size() != 0 || options.positionalValues.size() != 0)
        && params.size() != 0) {

      CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
      // Positional parameter case, ensure number of parameters is the same.
      if (options.positionalValues.size() != 0) {
        if (options.positionalValues.size() != params.size()) {
          return false;
        }
        Iterator<Object> primedPositionValues = params.values().iterator();
        Iterator<String> primedPositionTypes = paramTypes.values().iterator();
        // iterate over the parameters and make sure they all match
        for (ByteBuffer buffer : options.positionalValues) {
          if (!checkParamsEqual(
              buffer, primedPositionValues.next(), primedPositionTypes.next(), mapper)) {
            return false;
          }
        }
        // Named parameters case
      } else if (options.namedValues.size() != 0) {
        if (options.namedValues.size() != params.size()) {
          return false;
        }
        // look up each parameter and make sure they match
        for (String key : options.namedValues.keySet()) {
          String stringType = paramTypes.get(key);

          ByteBuffer buffer = options.namedValues.get(key);
          if (!checkParamsEqual(buffer, params.get(key), stringType, mapper)) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Query query1 = (Query) o;

    if (query != null ? !query.equals(query1.query) : query1.query != null) return false;
    return consistencyEnum != null
        ? consistencyEnum.equals(query1.consistencyEnum)
        : query1.consistencyEnum == null;
  }

  @Override
  public int hashCode() {
    int result = query != null ? query.hashCode() : 0;
    result = 31 * result + (consistencyEnum != null ? consistencyEnum.hashCode() : 0);
    result = 31 * result + (paramTypes != null ? paramTypes.hashCode() : 0);
    result = 31 * result + (params != null ? params.hashCode() : 0);
    return result;
  }

  /**
   * Convenience method to retrieve the queryId which happens to be the hashcode associated query
   * string
   *
   * @return the hashCode of the query text.
   */
  @JsonIgnore
  public int getQueryId() {
    return query.hashCode();
  }
}
