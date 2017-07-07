package com.datastax.simulacron.common.request;

import com.datastax.simulacron.common.codec.CqlMapper;
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
      if (params == null || params.size() == 0) {
        return true;
      }

      if (!query.equals(queryOrId)) {
        return false;
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
    } else {
      Integer queryIdInt = new BigInteger((byte[]) queryOrId).intValue();
      if (queryIdInt.equals(getQueryId())) {
        CqlMapper mapper = CqlMapper.forVersion(protocolVersion);

        Iterator<Object> primedPositionValues = params.values().iterator();
        Iterator<String> primedPositionTypes = paramTypes.values().iterator();

        for (ByteBuffer buffer : positionalValues) {
          if (!Request.checkParamsEqual(
              buffer, primedPositionValues.next(), primedPositionTypes.next(), mapper)) {
            return false;
          }
          return true;
        }
        return true;
      } else {
        return false;
      }
    }
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
