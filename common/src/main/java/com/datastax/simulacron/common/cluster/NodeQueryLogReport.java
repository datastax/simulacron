package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * Represent a class that contains the querylogs of a particular node. It's useful for encoding the
 * results with JSON.
 */
public class NodeQueryLogReport extends QueryLogReport
    implements NodeStructure<ClusterQueryLogReport, DataCenterQueryLogReport> {
  @JsonProperty("queries")
  private List<QueryLog> queryLogs;

  @JsonBackReference private final DataCenterQueryLogReport parent;

  NodeQueryLogReport() {
    // Default constructor for jackson deserialization.
    this(null, null, null);
  }

  public NodeQueryLogReport(Long id, List<QueryLog> queryLogs, DataCenterQueryLogReport parent) {

    super(id);
    this.queryLogs = queryLogs;
    this.parent = parent;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    NodeQueryLogReport that = (NodeQueryLogReport) o;
    if (!(this.getId().equals(that.getId()))) {
      return false;
    }
    return parent.equals(that.parent);
  }

  @Override
  public int hashCode() {
    return queryLogs != null ? queryLogs.hashCode() : 0;
  }

  @Override
  public ClusterQueryLogReport getRootReport() {
    return getCluster();
  }

  @Override
  public List<QueryLog> getQueryLogs() {
    return queryLogs;
  }

  @Override
  public DataCenterQueryLogReport getDataCenter() {
    return parent;
  }
}
