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
import com.datastax.oss.simulacron.common.stubbing.InternalStubMapping;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class ActivityLog {

  private List<QueryLog> queryLog = new ArrayList<QueryLog>();

  public QueryLog addLog(
      Frame frame, SocketAddress socketAddress, long timestamp, Optional<StubMapping> stubOption) {
    boolean isPrimed = false;
    if (stubOption.isPresent()) {
      StubMapping stub = stubOption.get();
      isPrimed = !(stub instanceof InternalStubMapping);
    }
    QueryLog log = new QueryLog(frame, socketAddress, timestamp, isPrimed, stubOption);
    queryLog.add(log);
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
