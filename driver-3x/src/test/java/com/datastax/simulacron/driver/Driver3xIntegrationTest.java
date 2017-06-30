package com.datastax.simulacron.driver;

import com.datastax.driver.core.Cluster;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.SimulacronCluster;
import com.datastax.simulacron.server.Server;
import com.datastax.simulacron.server.SimulacronServer;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.datastax.simulacron.driver.SimulacronDriverSupport.*;
import static org.assertj.core.api.Assertions.assertThat;

public class Driver3xIntegrationTest {

  private Server<SimulacronCluster> server = SimulacronServer.builder().build();

  @Test
  public void testShouldCreateAndConnectToCluster() throws Exception {
    try (SimulacronCluster sCluster =
            server.register(cluster().withNodes(3)).get(5, TimeUnit.SECONDS);
        Cluster cluster = defaultBuilder(sCluster).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(sCluster.getActiveConnections()).isEqualTo(4);
    }
  }

  @Test
  public void testShouldCreateAndConnectToNode() throws Exception {
    try (Node node = server.register(node()).get(5, TimeUnit.SECONDS);
        Cluster cluster = defaultBuilder(node).build()) {
      cluster.connect();

      // 1 connection for each host + control connection
      assertThat(node.getActiveConnections()).isEqualTo(2);
    }
  }
}
