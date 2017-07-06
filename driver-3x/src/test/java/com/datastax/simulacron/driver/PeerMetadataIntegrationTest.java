package com.datastax.simulacron.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.server.Server;
import com.datastax.simulacron.server.BoundCluster;
import org.junit.Test;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.datastax.simulacron.driver.SimulacronDriverSupport.cluster;
import static com.datastax.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class PeerMetadataIntegrationTest {

  private final Server server = Server.builder().build();

  @Test
  public void testClusterDiscovery() throws Exception {
    // Validate that peers as appropriately discovered when connecting to a node.
    BoundCluster boundCluster =
        server.register(cluster().withNodes(3, 3, 3)).get(5, TimeUnit.SECONDS);

    DataCenter dc0 = boundCluster.getDataCenters().iterator().next();

    try (Cluster driverCluster = defaultBuilder(boundCluster).build()) {
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
          dc0.getNodes().stream().map(Node::getAddress).collect(Collectors.toList());

      assertThat(connectedHosts).hasSameElementsAs(dcHosts);
    } finally {
      server.unregister(boundCluster.getId()).get(5, TimeUnit.SECONDS);
    }
  }
}
