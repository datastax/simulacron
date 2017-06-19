package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.Node;
import org.junit.After;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class AddressResolverIntegrationTest {

  private final Server server = Server.builder().build();

  @After
  public void tearDown() throws Exception {
    server.unregisterAll().get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testAddressesReused() throws Exception {
    // Validate that when a Cluster is unregistered, the ip addresses used can be reassigned to subsequently
    // created clusters.
    Cluster cluster0 = Cluster.builder().withNodes(3, 3, 3).build();
    Cluster boundCluster0 = server.register(cluster0).get(5, TimeUnit.SECONDS);

    // Collect addresses signed to cluster 0
    List<SocketAddress> cluster0Addrs =
        boundCluster0.getNodes().stream().map(Node::getAddress).collect(Collectors.toList());

    // Unregister cluster 0 which should free the ip addresses.
    server.unregister(boundCluster0.getId()).get(5, TimeUnit.SECONDS);

    // Register a new cluster.
    Cluster cluster1 = Cluster.builder().withNodes(4, 4, 1).build();
    Cluster boundCluster1 = server.register(cluster1).get(5, TimeUnit.SECONDS);

    // Collect addresses signed to cluster 0
    List<SocketAddress> cluster1Addrs =
        boundCluster1.getNodes().stream().map(Node::getAddress).collect(Collectors.toList());

    assertThat(cluster1Addrs).hasSameElementsAs(cluster0Addrs);
  }

  @Test
  public void testAddressesReassignedInSameOrder() throws Exception {
    // Validate that when a Cluster is unregistered, the ip addresses used can be reassigned to subsequently
    // created clusters.
    // Also affirms that the order of nodes and data centers within clusters is consistent.
    List<SocketAddress> lastAddresses = null;

    for (int i = 0; i < 10; i++) {
      Cluster cluster = Cluster.builder().withNodes(3, 3, 3).build();
      Cluster boundCluster = server.register(cluster).get(5, TimeUnit.SECONDS);

      List<SocketAddress> clusterAddrs =
          boundCluster.getNodes().stream().map(Node::getAddress).collect(Collectors.toList());

      server.unregister(boundCluster.getId()).get(5, TimeUnit.SECONDS);

      if (lastAddresses != null) {
        assertThat(clusterAddrs).isEqualTo(lastAddresses);
      }
      lastAddresses = clusterAddrs;
    }
  }
}
