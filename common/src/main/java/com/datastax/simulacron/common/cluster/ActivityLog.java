package com.datastax.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.simulacron.common.codec.ConsistencyLevel;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ActivityLog {

  private List<QueryLog> queryLog = new ArrayList<QueryLog>();

  public void addLog(
      Node node, Frame frame, SocketAddress socketAddress, long timestamp, boolean primed) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;

      queryLog.add(
          new QueryLog(
              node.getDataCenter().getId(),
              node.getId(),
              query.query,
              ConsistencyLevel.fromCode(query.options.consistency).name(),
              socketAddress.toString(),
              timestamp,
              primed));
    }
  }

  public void clearAll() {
    queryLog.clear();
  }

  public int getSize() {
    return queryLog.size();
  }

  public List<QueryLog> getLogs() {
    return queryLog;
  }

  public List<QueryLog> getLogs(QueryLogScope scope) {
    return queryLog
        .stream()
        .filter(log -> scope.isQueryLogInScope(log))
        .collect(Collectors.toList());
  }

  public void clearLogs(QueryLogScope scope) {
    Set<QueryLog> logsToClear =
        queryLog.stream().filter(log -> scope.isQueryLogInScope(log)).collect(Collectors.toSet());
    queryLog.removeAll(logsToClear);
  }
}
