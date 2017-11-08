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
package com.datastax.oss.simulacron.http.server;

import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.rows;
import static com.datastax.oss.simulacron.common.stubbing.PrimeDsl.when;
import static com.datastax.oss.simulacron.driver.DriverTypeAdapters.adapt;
import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.policies.FallthroughRetryPolicy;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.NodeQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.driver.SimulacronDriverSupport;
import com.datastax.oss.simulacron.server.Server;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.junit.Rule;
import org.junit.Test;

public class ActivityLogIntegrationTest {

  @Rule public AdminServer server = new AdminServer(ClusterSpec.builder().withNodes(3, 3).build());

  @Rule
  public AdminServer noActivityLogServer =
      new AdminServer(null, Server.builder().withActivityLoggingEnabled(false));

  @Test
  public void testVerifyQueriesFromCluster() throws Exception {
    String[] queries = new String[] {"select * from table1", "select * from table2"};
    primeAndExecuteQueries(queries, queries);

    List<QueryLog> queryLogs = getAllQueryLogs(server.getLogs(server.getCluster()));

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
      server.getCluster().clearLogs();
      for (String executeQuery : queries) {
        SimpleStatement stmt = new SimpleStatement(executeQuery);
        stmt.setDefaultTimestamp(100);
        session.execute(stmt);
      }
    }
  }

  @Test
  public void testVerifyQueriesFromDataCenterAndNode() throws Exception {
    String[] queries = new String[] {"select * from table1", "select * from table2"};
    primeAndExecuteQueries(queries, queries);
  }

  @Test
  public void testVerifyPrimedQueries() throws Exception {
    String[] primedQueries = new String[] {"select * from table1", "select * from table2"};
    String[] queries =
        new String[] {"select * from table1", "select * from table2", "select * from table3"};
    primeAndExecuteQueries(primedQueries, queries);
    //verify for cluster level filter: primed
    ClusterQueryLogReport queryLogReport =
        server.getLogs(server.getCluster().resolveId() + "?filter=primed");
    List<QueryLog> queryLogs = getAllQueryLogs(queryLogReport);
    assertThat(queryLogs).hasSize(2);

    //verify for cluster level filter: nonprimed
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().resolveId() + "?filter=nonprimed"));
    assertThat(queryLogs).hasSize(1);

    //verify for datacenter level filter: primed
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().resolveId() + "/dc1?filter=primed"));
    assertThat(queryLogs).hasSize(2);

    //verify for datacenter level filter: nonprimed
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().resolveId() + "/dc1?filter=nonprimed"));
    assertThat(queryLogs).hasSize(1);

    //verify for node level filter: primed
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().getName() + "/dc1/0?filter=primed"));
    assertThat(queryLogs).hasSize(1);

    //verify for node level filter: nonprimed
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().getName() + "/dc1/0?filter=nonprimed"));
    assertThat(queryLogs).hasSize(0);
  }

  @Test
  public void testShouldMarkPeerQueriesAsNonPrimed() throws Exception {
    server.getCluster().clearLogs();

    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster())
            .allowBetaProtocolVersion()
            .withRetryPolicy(FallthroughRetryPolicy.INSTANCE)
            .build()) {
      driverCluster.init();
    }
    //verify for node level filter: primed.  This should be empty as no primed queries were made other than internal.
    List<QueryLog> queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().getName() + "?filter=primed"));
    assertThat(queryLogs).isEmpty();

    // non primed should have the internal peer queries.
    queryLogs =
        getAllQueryLogs(server.getLogs(server.getCluster().getName() + "?filter=nonprimed"));
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

    List<QueryLog> queryLogs = getAllQueryLogs(server.getLogs(server.getCluster()));
    assertThat(queryLogs).isEmpty();
  }

  @Test
  public void testVerifyQueryLogInfo() throws Exception {
    long currentTimestamp = System.currentTimeMillis();
    String[] queries = new String[] {"select * from table1"};
    primeAndExecuteQueries(queries, queries);

    List<QueryLog> queryLogs = getAllQueryLogs(server.getLogs(server.getCluster()));
    assertThat(queryLogs.size()).isEqualTo(1);
    QueryLog log = queryLogs.get(0);
    assertThat(log.getConnection()).isNotNull();
    assertThat(log.getConsistency()).isEqualTo(adapt(ConsistencyLevel.LOCAL_ONE));
    assertThat(log.getReceivedTimestamp()).isNotZero();
    assertThat(log.getReceivedTimestamp()).isGreaterThan(currentTimestamp);
    assertThat(log.getClientTimestamp()).isEqualTo(100);
    assertThat(log.isPrimed()).isTrue();
  }

  private void shouldLogQuery(AdminServer server, String uri, String queryStr, boolean present)
      throws Exception {
    ClusterSpec cluster = server.mapTo(server.post(uri, null), ClusterSpec.class);
    try {
      query(queryStr, cluster);

      List<QueryLog> logs = getAllQueryLogs(server.getLogs(cluster));
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

  private void query(String statement, ClusterSpec cluster) throws Exception {
    try (com.datastax.driver.core.Cluster driverCluster =
        com.datastax.driver.core.Cluster.builder()
            .addContactPointsWithPorts(cluster.node(0).inetSocketAddress())
            .withNettyOptions(SimulacronDriverSupport.nonQuietClusterCloseOptions)
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

  private List<QueryLog> getAllQueryLogs(ClusterQueryLogReport report) {

    List<QueryLog> querylogs = new LinkedList<QueryLog>();
    for (DataCenterQueryLogReport dcReport : report.getDataCenters()) {
      for (NodeQueryLogReport nodeReport : dcReport.getNodes()) {
        if (nodeReport.getQueryLogs() != null) {
          querylogs.addAll(nodeReport.getQueryLogs());
        }
      }
    }
    return querylogs;
  }
}
