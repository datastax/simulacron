package com.datastax.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.simulacron.common.codec.ConsistencyLevel;
import com.datastax.simulacron.common.stubbing.InternalStubMapping;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.StubMapping;

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ActivityLog {

  private List<QueryLog> queryLog = new ArrayList<QueryLog>();

  public void addLog(
      Node node,
      Frame frame,
      SocketAddress socketAddress,
      Optional<StubMapping> stubOption,
      long timestamp) {
    // TODO: Add field which indicates the type of message.
    boolean isPrimed = false;
    if (stubOption.isPresent()) {
      StubMapping stub = stubOption.get();
      isPrimed = !(stub instanceof InternalStubMapping);
    }

    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;

      queryLog.add(
          new QueryLog(
              query.query,
              ConsistencyLevel.fromCode(query.options.consistency).name(),
              ConsistencyLevel.fromCode(query.options.serialConsistency).name(),
              socketAddress.toString(),
              timestamp,
              isPrimed));
    } else if (frame.message instanceof Execute) {
      Execute execute = (Execute) frame.message;
      if (stubOption.isPresent()) {
        StubMapping stub = stubOption.get();
        if (stub instanceof Prime) {
          Prime prime = (Prime) stub;
          if (prime.getPrimedRequest().when
              instanceof com.datastax.simulacron.common.request.Query) {
            com.datastax.simulacron.common.request.Query query =
                (com.datastax.simulacron.common.request.Query) prime.getPrimedRequest().when;
            queryLog.add(
                new QueryLog(
                    query.query,
                    ConsistencyLevel.fromCode(execute.options.consistency).name(),
                    ConsistencyLevel.fromCode(execute.options.serialConsistency).name(),
                    socketAddress.toString(),
                    timestamp,
                    isPrimed));
            return;
          }
        }
      }
      // TODO: Record unknown execute.
    }
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

  public List<QueryLog> getLogs(boolean primed) {
    return queryLog.stream().filter(l -> l.isPrimed() == primed).collect(Collectors.toList());
  }
}
