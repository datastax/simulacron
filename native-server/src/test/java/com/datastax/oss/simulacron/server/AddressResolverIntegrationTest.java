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
package com.datastax.oss.simulacron.server;

import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.net.SocketAddress;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class AddressResolverIntegrationTest {

  private final Server server = Server.builder().build();

  @After
  public void tearDown() throws Exception {
    server.unregisterAll();
  }

  @Test
  public void testAddressesReused() throws Exception {
    // Validate that when a Cluster is unregistered, the ip addresses used can be reassigned to subsequently
    // created clusters.
    ClusterSpec cluster0 = ClusterSpec.builder().withNodes(3, 3, 3).build();
    BoundCluster boundCluster0 = server.register(cluster0);

    // Collect addresses signed to cluster 0
    List<SocketAddress> cluster0Addrs =
        boundCluster0.getNodes().stream().map(BoundNode::getAddress).collect(Collectors.toList());

    // Unregister cluster 0 which should free the ip addresses.
    server.unregister(boundCluster0.getId());

    // Register a new cluster.
    ClusterSpec cluster1 = ClusterSpec.builder().withNodes(4, 4, 1).build();
    BoundCluster boundCluster1 = server.register(cluster1);

    // Collect addresses signed to cluster 0
    List<SocketAddress> cluster1Addrs =
        boundCluster1.getNodes().stream().map(BoundNode::getAddress).collect(Collectors.toList());

    assertThat(cluster1Addrs).hasSameElementsAs(cluster0Addrs);
  }

  @Test
  public void testAddressesReassignedInSameOrder() throws Exception {
    // Validate that when a Cluster is unregistered, the ip addresses used can be reassigned to subsequently
    // created clusters.
    // Also affirms that the order of nodes and data centers within clusters is consistent.
    List<SocketAddress> lastAddresses = null;

    for (int i = 0; i < 10; i++) {
      ClusterSpec cluster = ClusterSpec.builder().withNodes(3, 3, 3).build();
      BoundCluster boundCluster = server.register(cluster);

      List<SocketAddress> clusterAddrs =
          boundCluster.getNodes().stream().map(BoundNode::getAddress).collect(Collectors.toList());

      server.unregister(boundCluster.getId());

      if (lastAddresses != null) {
        assertThat(clusterAddrs).isEqualTo(lastAddresses);
      }
      lastAddresses = clusterAddrs;
    }
  }
}
