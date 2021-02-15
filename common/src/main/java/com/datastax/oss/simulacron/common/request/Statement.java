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

import com.datastax.oss.simulacron.common.codec.CqlMapper;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Statement {
  public final String query;
  public final Map<String, String> paramTypes;
  public final Map<String, Object> params;

  @JsonCreator
  public Statement(
      @JsonProperty("query") String query,
      @JsonProperty("param_types") Map<String, String> paramTypes,
      @JsonProperty("params") Map<String, Object> params) {
    this.query = query;
    this.paramTypes = paramTypes;
    this.params = params;
  }

  public boolean checkStatementMatch(
      int protocolVersion, Object queryOrId, List<ByteBuffer> positionalValues) {
    if (queryOrId instanceof String) {
      return query.equals(queryOrId) && checkStatementMatch(protocolVersion, positionalValues);
    } else {
      Integer queryIdInt = new BigInteger((byte[]) queryOrId).intValue();
      return queryIdInt.equals(getQueryId())
          && checkStatementMatch(protocolVersion, positionalValues);
    }
  }

  private boolean checkStatementMatch(int protocolVersion, List<ByteBuffer> positionalValues) {
    if (params == null || params.size() == 0) {
      return true;
    }

    if (positionalValues.size() != params.size()) {
      return false;
    }

    Iterator<Object> primedPositionValues = params.values().iterator();
    Iterator<String> primedPositionTypes = paramTypes.values().iterator();
    // iterate over the parameters and make sure they all match

    CqlMapper mapper = CqlMapper.forVersion(protocolVersion);
    for (ByteBuffer buffer : positionalValues) {
      if (!Request.checkParamsEqual(
          buffer, primedPositionValues.next(), primedPositionTypes.next(), mapper)) {
        return false;
      }
    }
    return true;
  }

  @JsonIgnore
  public int getQueryId() {
    return query.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Statement statement = (Statement) o;

    if (!query.equals(statement.query)) return false;
    if (!paramTypes.equals(statement.paramTypes)) return false;
    return params.equals(statement.params);
  }

  @Override
  public int hashCode() {
    int result = query.hashCode();
    result = 31 * result + paramTypes.hashCode();
    result = 31 * result + params.hashCode();
    return result;
  }
}
