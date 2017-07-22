/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.InternalStubMapping;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ActivityLog {

  private List<QueryLog> queryLog = new ArrayList<QueryLog>();

  public QueryLog addLog(
      Frame frame, SocketAddress socketAddress, Optional<StubMapping> stubOption, long timestamp) {
    // TODO: Add field which indicates the type of message.
    boolean isPrimed = false;
    if (stubOption.isPresent()) {
      StubMapping stub = stubOption.get();
      isPrimed = !(stub instanceof InternalStubMapping);
    }

    QueryLog log = null;

    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;

      log =
          new QueryLog(
              query.query,
              ConsistencyLevel.fromCode(query.options.consistency),
              ConsistencyLevel.fromCode(query.options.serialConsistency),
              socketAddress,
              timestamp,
              isPrimed);
    } else if (frame.message instanceof Options) {
      log = new QueryLog("OPTIONS", null, null, socketAddress, timestamp, stubOption.isPresent());
    } else if (frame.message instanceof Execute) {
      Execute execute = (Execute) frame.message;
      if (stubOption.isPresent()) {
        StubMapping stub = stubOption.get();
        if (stub instanceof Prime) {
          Prime prime = (Prime) stub;
          if (prime.getPrimedRequest().when
              instanceof com.datastax.oss.simulacron.common.request.Query) {
            com.datastax.oss.simulacron.common.request.Query query =
                (com.datastax.oss.simulacron.common.request.Query) prime.getPrimedRequest().when;
            log =
                new QueryLog(
                    query.query,
                    ConsistencyLevel.fromCode(execute.options.consistency),
                    ConsistencyLevel.fromCode(execute.options.serialConsistency),
                    socketAddress,
                    timestamp,
                    isPrimed);
          }
        }
      }
      // TODO: Record unknown execute.
    }
    if (log != null) {
      queryLog.add(log);
    }
    return log;
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
