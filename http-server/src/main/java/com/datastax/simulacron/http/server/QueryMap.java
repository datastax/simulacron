package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.QueryPrime;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

public class QueryMap {
  private static ConcurrentHashMap<QueryScopeId, LinkedList<QueryPrime>> queriesByScope =
      new ConcurrentHashMap<QueryScopeId, LinkedList<QueryPrime>>();

  public void putQuery(int clusterId, int dataCenterId, int nodeId, QueryPrime query) {
    queriesByScope
        .computeIfAbsent(new QueryScopeId(clusterId, dataCenterId, nodeId), v -> new LinkedList<>())
        .add(query);
  }

  public QueryPrime retrieveQuery(int clusterId, int dataCenterId, int nodeId, String query) {
    Optional<QueryPrime> optQuery =
        findQueryForScope(new QueryScopeId(clusterId, dataCenterId, nodeId), query);
    if (optQuery.isPresent()) return optQuery.get();
    optQuery = findQueryForScope(new QueryScopeId(clusterId, dataCenterId), query);
    if (optQuery.isPresent()) return optQuery.get();
    optQuery = findQueryForScope(new QueryScopeId(clusterId), query);
    if (optQuery.isPresent()) return optQuery.get();
    optQuery = findQueryForScope(new QueryScopeId(), query);
    if (optQuery.isPresent()) return optQuery.get();
    // log not found
    return null;
  }

  private Optional<QueryPrime> findQueryForScope(QueryScopeId id, String qString) {
    List<QueryPrime> qList = queriesByScope.get(id);
    return qList.stream().filter(q -> q.when.query.equals(qString)).findFirst();
  }

  private Optional<QueryPrime> findQueryForScopeWithRegex(QueryScopeId id, String qRegex) {
    List<QueryPrime> qList = queriesByScope.get(id);
    Pattern p = Pattern.compile(qRegex);
    return qList.stream().filter(q -> q.when.query.matches(qRegex)).findFirst();
  }

  private class QueryScopeId {
    int clusterId;
    int dataCenterId;
    int nodeId;

    QueryScopeId(int clusterId, int dataCenterId, int nodeId) {
      this.clusterId = clusterId;
      this.dataCenterId = dataCenterId;
      this.nodeId = nodeId;
    }

    QueryScopeId(int clusterId, int dataCenterId) {
      this(clusterId, dataCenterId, 0);
    }

    QueryScopeId(int clusterId) {
      this(clusterId, 0, 0);
    }

    QueryScopeId() {
      this(0, 0, 0);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      QueryScopeId queryId = (QueryScopeId) o;

      if (clusterId != queryId.clusterId) return false;
      if (dataCenterId != queryId.dataCenterId) return false;
      return nodeId == queryId.nodeId;
    }

    @Override
    public int hashCode() {
      int result = clusterId;
      result = 31 * result + dataCenterId;
      result = 31 * result + nodeId;
      return result;
    }
  }
}
