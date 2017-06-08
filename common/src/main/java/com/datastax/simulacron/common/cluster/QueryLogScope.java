package com.datastax.simulacron.common.cluster;

public class QueryLogScope {
  private Scope scope;
  private Boolean primed;

  public QueryLogScope(Scope scope, Boolean primed) {
    this.scope = scope;
    this.primed = primed;
  }

  boolean isQueryLogInScope(QueryLog queryLog) {
    if ((this.scope.getNodeId() == null)
        && (this.scope.getDatacenterId() == null)
        && primed == null) {
      return true;
    }

    boolean primeFilter = true;
    if (primed != null) {
      primeFilter = queryLog.isPrimed() == primed;
    }

    if (scope.getDatacenterId() == null) {
      return primeFilter;
    }

    boolean sameDatacenter = queryLog.getDatacenterId() == scope.getDatacenterId();
    if (this.scope.getNodeId() == null) {
      return primeFilter && sameDatacenter;
    }

    boolean sameNode = queryLog.getNodeId() == scope.getNodeId();
    return primeFilter && sameDatacenter && sameNode;
  }
}
