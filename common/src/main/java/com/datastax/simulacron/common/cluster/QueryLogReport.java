package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;

public abstract class QueryLogReport extends AbstractIdentifiable {
  QueryLogReport(Long id) {
    super(id);
  }

  @JsonIgnore
  public abstract ClusterQueryLogReport getRootReport();

  @JsonIgnore
  public abstract List<QueryLog> getQueryLogs();
}
