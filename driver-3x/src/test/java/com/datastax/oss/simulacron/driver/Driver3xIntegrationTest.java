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
package com.datastax.oss.simulacron.driver;

import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.exceptions.UnsupportedProtocolVersionException;
import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import org.junit.Test;

public class Driver3xIntegrationTest {

  private Server server = Server.builder().build();

  @Test
  public void testShouldCreateAndConnectToCluster() {
    try (BoundCluster sCluster = server.register(ClusterSpec.builder().withNodes(3));
        Cluster cluster = defaultBuilder(sCluster).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(sCluster.getActiveConnections()).isEqualTo(4);
    }
  }

  @Test
  public void testShouldCreateAndConnectToNode() {
    try (BoundNode node = server.register(NodeSpec.builder().build());
        Cluster cluster = defaultBuilder(node).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(node.getActiveConnections()).isEqualTo(2);
    }
  }

  @Test
  public void testShouldFailToConnectWithOlderProtocolVersion() {
    try (BoundNode node = server.register(NodeSpec.builder().build());
        Cluster cluster = defaultBuilder(node).withProtocolVersion(ProtocolVersion.V2).build()) {
      // Since simulacron does not support < V3, an exception should be thrown if we try to force
      // an older version.
      try {
        cluster.connect();
      } catch (UnsupportedProtocolVersionException e) {
        // expected
      }

      // Should get a query log indicating invalid protocol version was used.
      assertThat(node.getLogs().getQueryLogs()).hasSize(1);
      QueryLog log = node.getLogs().getQueryLogs().get(0);
      Frame frame = log.getFrame();
      assertThat(frame.protocolVersion).isEqualTo(2);
      assertThat(frame.warnings).hasSize(1);
      assertThat(frame.warnings.get(0))
          .isEqualTo(
              "This message contains a non-supported protocol version by this node.  STARTUP is inferred, but may not reflect the actual message sent.");
      assertThat(frame.message).isInstanceOf(Startup.class);
    }
  }
}
