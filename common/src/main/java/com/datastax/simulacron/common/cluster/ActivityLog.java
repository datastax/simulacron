package com.datastax.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ActivityLog {

  private List<QueryLog> queryLog = new ArrayList<QueryLog>();

  public void addLog(Node node, Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      queryLog.add(
          new QueryLog(
              node.getDataCenter().getId(), node.getId(), query.query, query.options.consistency));
    }
  }

  public List<QueryLog> getLogsFromNode(long datacenterId, long nodeId) {
    return queryLog
        .stream()
        .filter(log -> log.getDatacenterId() == datacenterId && log.getNodeId() == nodeId)
        .collect(Collectors.toList());
  }

  public List<QueryLog> getLogsFromDatacenter(long datacenterId) {
    return queryLog
        .stream()
        .filter(log -> log.getDatacenterId() == datacenterId)
        .collect(Collectors.toList());
  }

  public void clear() {
    queryLog.clear();
  }

  public int getSize() {
    return queryLog.size();
  }

  public List<QueryLog> getLogs() {
    return queryLog;
  }
}
