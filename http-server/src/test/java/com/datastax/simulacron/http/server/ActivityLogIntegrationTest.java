package com.datastax.simulacron.http.server;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.server.Server;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

import static com.datastax.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.simulacron.test.IntegrationUtils.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class ActivityLogIntegrationTest {

  @Rule public AdminServer server = new AdminServer(Cluster.builder().withNodes(3, 3).build());

  @Rule
  public AdminServer noActivityLogServer =
      new AdminServer(null, Server.builder().withActivityLoggingEnabled(false));

  @Test
  public void testVerifyQueriesFromCluster() throws Exception {
    String[] queries = new String[] {"select * from table1", "select * from table2"};
    primeAndExecuteQueries(queries, queries);

    List<QueryLog> queryLogs = server.getLogs(server.getCluster());

    assertThat(queryLogs).hasSize(2);
  }

  private void primeAndExecuteQueries(String[] primed, String[] queries) throws Exception {
    SuccessResult result = getSampleSuccessResult();
    for (String primeQuery : primed) {
      server.prime(when(primeQuery).then(result));
    }

    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      Session session = driverCluster.connect();
      server.getCluster().getActivityLog().clearAll();
      for (String executeQuery : queries) {
        session.execute(new SimpleStatement(executeQuery));
      }
    }
  }

  @Test
  public void testVerifyQueriesFromDataCenterAndNode() throws Exception {
    String[] queries = new String[] {"select * from table1", "select * from table2"};
    primeAndExecuteQueries(queries, queries);
    List<QueryLog> queryLogs = server.getLogs(server.getCluster().resolveId() + "/dc1");

    assertThat(queryLogs).hasSize(2);

    queryLogs = server.getLogs(server.getCluster().getName() + "/dc2/node1");

    assertThat(queryLogs).hasSize(0);
  }

  @Test
  public void testVerifyPrimedQueries() throws Exception {
    String[] primedQueries = new String[] {"select * from table1", "select * from table2"};
    String[] queries =
        new String[] {"select * from table1", "select * from table2", "select * from table3"};
    primeAndExecuteQueries(primedQueries, queries);
    //verify for cluster level filter: primed
    List<QueryLog> queryLogs = server.getLogs(server.getCluster().resolveId() + "?filter=primed");
    assertThat(queryLogs).hasSize(2);

    //verify for cluster level filter: nonprimed
    queryLogs = server.getLogs(server.getCluster().resolveId() + "?filter=nonprimed");
    assertThat(queryLogs).hasSize(1);

    //verify for datacenter level filter: primed
    queryLogs = server.getLogs(server.getCluster().resolveId() + "/dc1?filter=primed");
    assertThat(queryLogs).hasSize(2);

    int expectedPrimedForNode1 = (int) queryLogs.stream().filter(q -> q.getNodeId() == 0).count();

    //verify for datacenter level filter: nonprimed
    queryLogs = server.getLogs(server.getCluster().resolveId() + "/dc1?filter=nonprimed");
    assertThat(queryLogs).hasSize(1);

    int expectedNonPrimedForNode1 =
        (int) queryLogs.stream().filter(q -> q.getNodeId() == 0).count();

    //verify for node level filter: primed
    queryLogs = server.getLogs(server.getCluster().getName() + "/dc1/0?filter=primed");
    assertThat(queryLogs).hasSize(expectedPrimedForNode1);

    //verify for node level filter: nonprimed
    queryLogs = server.getLogs(server.getCluster().getName() + "/dc1/0?filter=nonprimed");
    assertThat(queryLogs).hasSize(expectedNonPrimedForNode1);
  }

  @Test
  public void testShouldMarkPeerQueriesAsNonPrimed() throws Exception {
    server.getCluster().getActivityLog().clearAll();

    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .allowBetaProtocolVersion()
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      driverCluster.init();
    }
    // verify for node level filter: primed.  This should be empty as no primed queries were made other than internal.
    List<QueryLog> queryLogs = server.getLogs(server.getCluster().getName() + "?filter=primed");
    assertThat(queryLogs).isEmpty();

    // non primed should have the internal peer queries.
    queryLogs = server.getLogs(server.getCluster().getName() + "?filter=nonprimed");
    assertThat(queryLogs).isNotEmpty();
  }

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

  @Test
  public void testClearQueryLog() throws Exception {
    String[] queries = new String[] {"select * from table1", "select * from table2"};
    primeAndExecuteQueries(queries, queries);

    server.delete("/log/" + server.getCluster().resolveId());

    List<QueryLog> queryLogs = server.getLogs(server.getCluster());
    assertThat(queryLogs).isEmpty();
  }

  @Test
  public void testVerifyQueryLogInfo() throws Exception {
    long currentTimestamp = System.currentTimeMillis();
    String[] queries = new String[] {"select * from table1"};
    primeAndExecuteQueries(queries, queries);

    List<QueryLog> queryLogs = server.getLogs(server.getCluster());
    assertThat(queryLogs.size()).isEqualTo(1);
    QueryLog log = queryLogs.get(0);
    assertThat(log.getConnection()).isNotNull();
    assertThat(ConsistencyLevel.valueOf(log.getConsistency()))
        .isEqualTo(ConsistencyLevel.LOCAL_ONE);
    assertThat(log.getTimestamp()).isNotZero();
    assertThat(log.getTimestamp()).isGreaterThan(currentTimestamp);
    assertThat(log.isPrimed()).isTrue();
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

  private SuccessResult getSampleSuccessResult() {
    return rows()
        .row("column1", "column1", "column2", 2)
        .columnTypes("column1", "ascii", "column2", "bigint")
        .build();
  }
}
