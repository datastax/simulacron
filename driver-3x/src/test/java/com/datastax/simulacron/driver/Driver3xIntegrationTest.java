package com.datastax.simulacron.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.simulacron.server.BoundCluster;
import com.datastax.simulacron.server.BoundNode;
import com.datastax.simulacron.server.Server;
import org.junit.Test;

import static com.datastax.simulacron.driver.SimulacronDriverSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

public class Driver3xIntegrationTest {

  private Server server = Server.builder().build();

  @Test
  public void testShouldCreateAndConnectToCluster() throws Exception {
    try (BoundCluster sCluster = server.register(cluster().withNodes(3));
        Cluster cluster = defaultBuilder(sCluster).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(sCluster.getActiveConnections()).isEqualTo(4);
    }
  }

  @Test
  public void testShouldCreateAndConnectToNode() throws Exception {
    try (BoundNode node = server.register(node());
        Cluster cluster = defaultBuilder(node).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(node.getActiveConnections()).isEqualTo(2);
    }
  }
}
