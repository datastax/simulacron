/*
 * Copyright DataStax, Inc.
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

import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.oss.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.server.BoundDataCenter;
import com.datastax.oss.simulacron.server.BoundNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.junit.Rule;
import org.junit.Test;

public class EndpointIntegrationTest {
  private final ObjectMapper om = ObjectMapperHolder.getMapper();

  @Rule public AdminServer server = new AdminServer(ClusterSpec.builder().withNodes(3, 3).build());

  @Test
  public void testGetConnections() throws Exception {
    Collection<BoundDataCenter> datacenters = server.getCluster().getDataCenters();
    BoundDataCenter dc = datacenters.iterator().next();
    Iterator<BoundNode> nodeIterator = dc.getNodes().iterator();
    BoundNode node = nodeIterator.next();

    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder(server.getCluster()).build()) {
      driverCluster.init();

      assertThat(driverCluster.getMetadata().getAllHosts()).hasSize(6);
      driverCluster.connect();

      ArrayList<Scope> list = new ArrayList<>();
      list.add(new Scope(server.getCluster().getId(), dc.getId(), node.getId()));
      list.add(new Scope(server.getCluster().getId(), dc.getId(), null));
      list.add(new Scope(server.getCluster().getId(), null, null));

      for (Scope scope : list) {
        HttpTestResponse responseToValidate = server.get("/connections/" + scope.toString());
        assertThat(responseToValidate.response.statusCode()).isEqualTo(200);
        ClusterConnectionReport responseReport =
            om.readValue(responseToValidate.body, ClusterConnectionReport.class);
        this.verifyClusterConnectionReport(responseReport, scope, dc.getId(), node.getId());
      }
    }
  }

  @Test
  public void testDeleteConnections() throws Exception {
    Collection<BoundDataCenter> datacenters = server.getCluster().getDataCenters();
    BoundDataCenter dc = datacenters.iterator().next();
    Iterator<BoundNode> nodeIterator = dc.getNodes().iterator();
    BoundNode node = nodeIterator.next();

    ArrayList<Scope> list = new ArrayList<>();
    list.add(new Scope(server.getCluster().getId(), dc.getId(), node.getId()));
    list.add(new Scope(server.getCluster().getId(), dc.getId(), null));
    list.add(new Scope(server.getCluster().getId(), null, null));

    for (Scope scope : list) {
      try (com.datastax.driver.core.Cluster driverCluster =
          defaultBuilder()
              .addContactPointsWithPorts((InetSocketAddress) node.getAddress())
              .build()) {
        driverCluster.init();
        HttpTestResponse responseDelete =
            server.delete("/connections/" + scope.toString() + "?type=disconnect");

        ClusterConnectionReport responseReport =
            om.readValue(responseDelete.body, ClusterConnectionReport.class);
        Collection<NodeConnectionReport> nodes =
            getNodeConnectionReports(responseReport, dc.getId());

        assertThat(responseDelete.response.statusCode()).isEqualTo(200);

        HttpTestResponse responseNewConnections = server.get("/connections/" + scope.toString());
        assertThat(responseNewConnections.body).isNotEqualTo(responseDelete.body);
        for (NodeConnectionReport nodeReport : nodes) {
          for (SocketAddress sA : nodeReport.getConnections()) {
            String sAString = sA.toString();
            assertThat(responseDelete.body).contains(sAString.substring(1, sAString.length()));
            assertThat(responseNewConnections.body)
                .doesNotContain(sAString.substring(1, sAString.length()));
          }
        }
      }
    }
  }

  @Test
  public void testDeleteParticularConnection() throws Exception {
    Collection<BoundDataCenter> datacenters = server.getCluster().getDataCenters();
    BoundDataCenter dc = datacenters.iterator().next();
    Iterator<BoundNode> nodeIterator = dc.getNodes().iterator();
    BoundNode node = nodeIterator.next();

    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder().addContactPointsWithPorts((InetSocketAddress) node.getAddress()).build()) {
      driverCluster.init();

      assertThat(driverCluster.getMetadata().getAllHosts()).hasSize(6);
      driverCluster.connect();

      Scope scope = new Scope(server.getCluster().getId(), dc.getId(), node.getId());
      HttpTestResponse response = server.get("/connections/" + scope.toString());

      ClusterConnectionReport clusterReport =
          om.readValue(response.body, ClusterConnectionReport.class);
      NodeConnectionReport connectionNode =
          clusterReport
              .getDataCenters()
              .stream()
              .filter(dce -> dce.getId().equals(dc.getId()))
              .flatMap(d -> d.getNodes().stream())
              .filter(n -> n.getId().equals(node.getId()))
              .findAny()
              .get();
      InetSocketAddress connection = (InetSocketAddress) connectionNode.getConnections().get(0);

      HttpTestResponse deleteResponse =
          server.delete(
              "/connection/"
                  + server.getCluster().getId()
                  + connection.getAddress()
                  + "/"
                  + connection.getPort());
      assertThat(deleteResponse.response.statusCode()).isEqualTo(200);

      HttpTestResponse responseAfter = server.get("/connections/" + scope.toString());

      String sAString = connection.getAddress() + ":" + connection.getPort();
      String sAtrimmed = sAString.substring(1, sAString.length());
      assertThat(responseAfter.body).isNotEqualTo(response.body);
      assertThat(responseAfter.body).doesNotContain(sAtrimmed);
      assertThat(deleteResponse.body).contains(sAtrimmed);
    }
  }

  @Test
  public void testDeleteParticularConnectionNotFound() throws Exception {
    // 888 would be root reserved port so unused.
    HttpTestResponse deleteResponse =
        server.delete("/connection/" + server.getCluster().getId() + "/127.0.0.1/888");
    assertThat(deleteResponse.response.statusCode()).isEqualTo(404);
  }

  @Test
  public void testDeleteParticularConnectionBadPort() throws Exception {
    // Invalid port should return bad request.
    HttpTestResponse deleteResponse =
        server.delete("/connection/" + server.getCluster().getId() + "/127.0.0.1/8888888");
    assertThat(deleteResponse.response.statusCode()).isEqualTo(400);
  }

  @Test
  public void testRejectAndAcceptConnections() throws Exception {
    Collection<BoundDataCenter> datacenters = server.getCluster().getDataCenters();
    Iterator<BoundDataCenter> dcI = datacenters.iterator();
    BoundDataCenter dc = dcI.next();
    BoundDataCenter dc2 = dcI.next();
    BoundNode node = dc.getNodes().iterator().next();
    BoundNode node2 = dc2.getNodes().iterator().next();

    ArrayList<Scope> list = new ArrayList<>();
    list.add(new Scope(server.getCluster().getId(), dc.getId(), node.getId()));
    list.add(new Scope(server.getCluster().getId(), dc.getId(), null));
    list.add(new Scope(server.getCluster().getId(), null, null));

    for (Scope scope : list) {
      HttpTestResponse delete =
          server.delete("/listener/" + scope + "?after=" + 0 + "&type=" + "unbind");
      assertThat(delete.response.statusCode()).isEqualTo(200);

      com.datastax.driver.core.Cluster driverCluster = null;
      try {
        if (scope.getClusterId() == null) {
          driverCluster =
              defaultBuilder()
                  .addContactPointsWithPorts((InetSocketAddress) node.getAddress())
                  .build();
        } else {
          driverCluster =
              defaultBuilder()
                  .addContactPointsWithPorts((InetSocketAddress) node2.getAddress())
                  .build();
        }
        driverCluster.init();
      } catch (NoHostAvailableException e) {
      } finally {
        if (driverCluster != null) {
          driverCluster.close();
        }
      }

      HttpTestResponse accept = server.put("/listener/" + scope);
      assertThat(accept.response.statusCode()).isEqualTo(200);

      try (com.datastax.driver.core.Cluster driverCluster2 =
          defaultBuilder()
              .addContactPointsWithPorts((InetSocketAddress) node.getAddress())
              .build()) {
        driverCluster2.init();
        driverCluster2.close();
      }
    }
  }

  @Test
  public void testRejectAndAcceptAfter() throws Exception {
    Collection<BoundDataCenter> datacenters = server.getCluster().getDataCenters();
    BoundDataCenter dc = datacenters.iterator().next();
    Iterator<BoundNode> nodeIterator = dc.getNodes().iterator();
    BoundNode node = nodeIterator.next();

    Scope scope = new Scope(server.getCluster().getId(), dc.getId(), node.getId());
    HttpTestResponse delete =
        server.delete("/listener/" + scope + "?after=" + 3 + "&type=" + "unbind");
    assertThat(delete.response.statusCode()).isEqualTo(200);

    // First try
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder().addContactPointsWithPorts((InetSocketAddress) node.getAddress()).build()) {
      driverCluster.init();
    }

    // Second try
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder().addContactPointsWithPorts((InetSocketAddress) node.getAddress()).build()) {
      driverCluster.init();
    }

    // Now it should be rejected
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder().addContactPointsWithPorts((InetSocketAddress) node.getAddress()).build()) {
      driverCluster.init();
    } catch (NoHostAvailableException e) {
    }

    HttpTestResponse accept = server.put("/listener/" + scope);
    assertThat(accept.response.statusCode()).isEqualTo(200);

    // Now it should go back to normal
    try (com.datastax.driver.core.Cluster driverCluster =
        defaultBuilder().addContactPointsWithPorts((InetSocketAddress) node.getAddress()).build()) {
      driverCluster.init();
    }
  }

  private Collection<NodeConnectionReport> getNodeConnectionReports(
      ClusterConnectionReport report, Long contactPointDcId) {
    DataCenterConnectionReport dc =
        report
            .getDataCenters()
            .stream()
            .filter(p -> p.getId().equals(contactPointDcId))
            .findAny()
            .get();
    return dc.getNodes();
  }

  private void verifyClusterConnectionReport(
      ClusterConnectionReport report, Scope scope, Long contactPointDcId, Long contactPointNodeId) {
    DataCenterConnectionReport dc =
        report
            .getDataCenters()
            .stream()
            .filter(p -> p.getId().equals(contactPointDcId))
            .findAny()
            .get();
    if (scope.getNodeId() != null) {
      assertThat(dc.getNodes().size()).isEqualTo(1);
    } else {
      assertThat(dc.getNodes().size()).isEqualTo(3);
    }
    for (NodeConnectionReport node : dc.getNodes()) {
      if (node.getId().equals(contactPointNodeId)) {
        assertThat(node.getConnections().size()).isEqualTo(2);
      } else {
        assertThat(node.getConnections().size()).isEqualTo(1);
      }
    }
  }
}
