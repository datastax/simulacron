package com.datastax.simulacron.common.cluster;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.result.Result;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public final class QueryPrime {
  public final When when;
  public final Result then;

  @JsonCreator
  public QueryPrime(@JsonProperty("when") When when, @JsonProperty("then") Result then) {
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
      this.consistencyEnum = createEnumFromConsistency(consistency);
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

  private static List<ConsistencyLevel> createEnumFromConsistency(String[] consistencies) {
    if (consistencies == null) {
      return new LinkedList<ConsistencyLevel>();
    }
    List<ConsistencyLevel> consistencyEnum = new LinkedList<ConsistencyLevel>();
    for (String consistency : consistencies) {
      consistencyEnum.add(ConsistencyLevel.fromString(consistency));
    }
    return consistencyEnum;
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
