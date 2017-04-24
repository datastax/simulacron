package com.datastax.simulacron.common.cluster;

import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/** Created by gregbestland on 4/3/17. */
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
  }
}
