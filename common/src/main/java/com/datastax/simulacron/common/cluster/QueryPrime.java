package com.datastax.simulacron.common.cluster;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public final class QueryPrime {
  public final When when;
  public final Then then;

  @JsonCreator
  public QueryPrime(@JsonProperty("when") When when, @JsonProperty("then") Then then) {
    this.when = when;
    this.then = then;
  }

  public static final class When {
    public final String query;
    public final String[] consistency;
    public final List<ConsistencyLevel> consistencyEnum;

    @JsonCreator
    public When(
        @JsonProperty("query") String query, @JsonProperty("consistency") String[] consistency) {
      this.query = query;
      this.consistency = consistency;
      this.consistencyEnum = createEnumFromConsitency(consistency);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      When when = (When) o;

      if (query != null ? !query.equals(when.query) : when.query != null) return false;
      // Probably incorrect - comparing Object[] arrays with Arrays.equals
      if (!Arrays.equals(consistency, when.consistency)) return false;
      return consistencyEnum != null
          ? consistencyEnum.equals(when.consistencyEnum)
          : when.consistencyEnum == null;
    }

    @Override
    public int hashCode() {
      int result = query != null ? query.hashCode() : 0;
      result = 31 * result + Arrays.hashCode(consistency);
      result = 31 * result + (consistencyEnum != null ? consistencyEnum.hashCode() : 0);
      return result;
    }
  }

  private static List<ConsistencyLevel> createEnumFromConsitency(String[] consistencies) {
    if (consistencies == null) {
      return new LinkedList<ConsistencyLevel>();
    }
    List<ConsistencyLevel> consistencyEnum = new LinkedList<ConsistencyLevel>();
    for (String consistency : consistencies) {
      consistencyEnum.add(ConsistencyLevel.fromString(consistency));
    }
    return consistencyEnum;
  }

  public static final class Then {
    public final List<Map<String, Object>> rows;
    public final String result;
    public final Map<String, String> column_types;

    @JsonCreator
    public Then(
        @JsonProperty("rows") List<Map<String, Object>> rows,
        @JsonProperty("result") String result,
        @JsonProperty("column_types") Map<String, String> column_types) {

      this.rows = rows;
      this.result = result;
      this.column_types = column_types;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Then then = (Then) o;

      if (rows != null ? !rows.equals(then.rows) : then.rows != null) return false;
      if (result != null ? !result.equals(then.result) : then.result != null) return false;
      return column_types != null
          ? column_types.equals(then.column_types)
          : then.column_types == null;
    }

    @Override
    public int hashCode() {
      int result1 = rows != null ? rows.hashCode() : 0;
      result1 = 31 * result1 + (result != null ? result.hashCode() : 0);
      result1 = 31 * result1 + (column_types != null ? column_types.hashCode() : 0);
      return result1;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    QueryPrime that = (QueryPrime) o;

    if (when != null ? !when.equals(that.when) : that.when != null) return false;
    return then != null ? then.equals(that.then) : that.then == null;
  }

  @Override
  public int hashCode() {
    int result = when != null ? when.hashCode() : 0;
    result = 31 * result + (then != null ? then.hashCode() : 0);
    return result;
  }
}
