package com.datastax.simulacron.http.server;

import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.server.Server;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.datastax.simulacron.test.IntegrationUtils.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class ActivityLogIntegrationTest {

  @Rule public AdminServer server = new AdminServer(null);

  @Rule
  public AdminServer noActivityLogServer =
      new AdminServer(null, Server.builder().withActivityLoggingEnabled(false));

  @Test
  public void testShouldLogByDefault() throws Exception {
    shouldLogQuery(server, "/cluster?data_centers=1", "select * from shouldLogByDefault", true);
  }

  @Test
  public void testShouldDisableLogWhenRequested() throws Exception {
    shouldLogQuery(
        server,
        "/cluster?data_centers=1&activity_log=false",
        "select * from shouldDisableLogWhenRequested",
        false);
  }

  @Test
  public void testShouldNotLogWhenDisabledOnServer() throws Exception {
    shouldLogQuery(
        noActivityLogServer,
        "/cluster?data_centers=1",
        "select * from shouldNotLogWhenDisabledOnServer",
        false);
  }

  @Test
  public void testShouldEnableLogWhenRequested() throws Exception {
    shouldLogQuery(
        noActivityLogServer,
        "/cluster?data_centers=1&activity_log=true",
        "select * from shouldEnableLogWhenRequested",
        true);
  }

  private void shouldLogQuery(AdminServer server, String uri, String queryStr, boolean present)
      throws Exception {
    Cluster cluster = server.mapTo(server.post(uri, null), Cluster.class);
    try {
      query(queryStr, cluster);

      List<QueryLog> logs = server.getLogs(cluster);
      if (present) {
        Optional<QueryLog> queryLog =
            logs.stream().filter(l -> l.getQuery().equals(queryStr)).findFirst();
        assertThat(queryLog).isPresent();
      } else {
        assertThat(logs).isEmpty();
      }
    } finally {
      server.delete("/cluster/" + cluster.getId());
    }
  }

  private void query(String statement, Cluster cluster) throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(cluster)
            .allowBetaProtocolVersion()
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      driverCluster.connect().execute(statement);
    }
  }
}
