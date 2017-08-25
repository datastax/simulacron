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
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Token;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.BoundDataCenter;
import com.datastax.oss.simulacron.server.BoundNode;
import com.datastax.oss.simulacron.server.Server;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Test;

public class PeerMetadataIntegrationTest {

  private final Server server = Server.builder().build();

  @Test
  public void testClusterDiscovery() throws Exception {
    // Validate that peers as appropriately discovered when connecting to a node.
    try (BoundCluster boundCluster = server.register(ClusterSpec.builder().withNodes(3, 3, 3));
        Cluster driverCluster = defaultBuilder(boundCluster).build()) {
      BoundDataCenter dc0 = boundCluster.getDataCenters().iterator().next();
      driverCluster.init();

      // Should be 9 hosts
      assertThat(driverCluster.getMetadata().getAllHosts()).hasSize(9);

      // Connect and ensure pools are created to local dc hosts.
      Session session = driverCluster.connect();

      // Verify hosts connected to are only those in the local DC.
      Collection<SocketAddress> connectedHosts =
          session
              .getState()
              .getConnectedHosts()
              .stream()
              .map(Host::getSocketAddress)
              .collect(Collectors.toList());

      Collection<SocketAddress> dcHosts =
          dc0.getNodes().stream().map(BoundNode::getAddress).collect(Collectors.toList());

      assertThat(connectedHosts).hasSameElementsAs(dcHosts);
    }
  }

  @Test
  public void testVnodeSupport() throws Exception {
    // Validate that peers as appropriately discovered when connecting to a node and vnodes are assigned.
    try (BoundCluster boundCluster =
            server.register(ClusterSpec.builder().withNumberOfTokens(256).withNodes(3, 3, 3));
        Cluster driverCluster = defaultBuilder(boundCluster).build()) {
      driverCluster.init();

      // Should be 9 hosts
      assertThat(driverCluster.getMetadata().getAllHosts()).hasSize(9);

      Set<Token> allTokens = new HashSet<>();
      for (Host host : driverCluster.getMetadata().getAllHosts()) {
        assertThat(host.getTokens()).hasSize(256);
        allTokens.addAll(host.getTokens());
      }

      // Should be 256*9 unique tokens.
      assertThat(allTokens).hasSize(256 * 9);
    }
  }
}
