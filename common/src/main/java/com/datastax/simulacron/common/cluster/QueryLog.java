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
  private String consistency;

  @JsonProperty("connection")
  private String connection;

  @JsonProperty("timestamp")
  private long timestamp;

  @JsonProperty("primed")
  private boolean primed;

  @JsonCreator
  public QueryLog(
      @JsonProperty("datacenter_id") long datacenterId,
      @JsonProperty("node_id") long nodeId,
      @JsonProperty("query") String query,
      @JsonProperty("consistency_level") String consistency,
      @JsonProperty("connection") String connection,
      @JsonProperty("timestamp") long timestamp,
      @JsonProperty("primed") boolean primed) {
    this.datacenterId = datacenterId;
    this.nodeId = nodeId;
    this.query = query;
    this.consistency = consistency;
    this.connection = connection;
    this.timestamp = timestamp;
    this.primed = primed;
  }

  public long getNodeId() {
    return nodeId;
  }

  public String getQuery() {
    return query;
  }

  public String getConsistency() {
    return consistency;
  }

  public long getDatacenterId() {
    return datacenterId;
  }

  public String getConnection() {
    return connection;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isPrimed() {
    return primed;
  }
}
