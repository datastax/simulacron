package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

public abstract class QueryLogReport extends AbstractReport {
  QueryLogReport(Long id) {
    super(id);
  }

  @JsonIgnore
  public abstract ClusterQueryLogReport getRootReport();
}
