package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryLog {
  @JsonProperty("datacenter_id")
  private long datacenterId;

  @JsonProperty("node_id")
  private long nodeId;

  @JsonProperty("query")
  private String query;

  @JsonProperty("consistency_level")
  private int consistency;

  @JsonCreator
  public QueryLog(
      @JsonProperty("datacenter_id") long datacenterId,
      @JsonProperty("node_id") long nodeId,
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") int consistency) {
    this.datacenterId = datacenterId;
    this.nodeId = nodeId;
    this.query = query;
    this.consistency = consistency;
  }

  public long getNodeId() {
    return nodeId;
  }

  public String getQuery() {
    return query;
  }

  public int getConsistency() {
    return consistency;
  }

  public long getDatacenterId() {
    return datacenterId;
  }
}
