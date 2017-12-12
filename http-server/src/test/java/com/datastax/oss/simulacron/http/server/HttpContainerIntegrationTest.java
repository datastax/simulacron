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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpContainerIntegrationTest {
  private HttpContainer httpContainer;
  private Vertx vertx = null;
  private int portNum = 8187;
  private Server nativeServer;
  ObjectMapper om = ObjectMapperHolder.getMapper();
  Logger logger = LoggerFactory.getLogger(HttpContainerIntegrationTest.class);

  @Before
  public void setup() throws Exception {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(portNum, true);
    nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
    QueryManager qManager = new QueryManager(nativeServer);
    qManager.registerWithRouter(httpContainer.getRouter());
    ActivityLogManager logManager = new ActivityLogManager(nativeServer);
    logManager.registerWithRouter(httpContainer.getRouter());
    EndpointManager endpointManager = new EndpointManager(nativeServer);
    endpointManager.registerWithRouter(httpContainer.getRouter());
    httpContainer.start().get(10, TimeUnit.SECONDS);
  }

  @After
  public void tearDown() {

    httpContainer.stop();
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.close(
        res -> {
          future.complete(null);
        });
    try {
      future.get();
    } catch (Exception e) {
      logger.error("Error encountered during cleanup", e);
      fail("Error encountered during cleanup");
    }

    try {
      nativeServer.unregisterAll();
    } catch (Exception e) {
      logger.error("Error encountered unregistering native server clusters", e);
    }

    portNum++;
  }

  @Test
  public void testClusterCreationSimple() {
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=1",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    String body = totalBuffer.toString();
                    HttpTestResponse testResponse = new HttpTestResponse(response, body);
                    future.complete(testResponse);
                  });
            })
        .end();

    try {
      HttpTestResponse responseToValidate = future.get();
      ObjectMapper om = ObjectMapperHolder.getMapper();
      // create cluster object from json return code
      ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      assertThat(new Long(0)).isEqualTo(cluster.getId());
      assertThat("0").isEqualTo(cluster.getName());
      assertThat(cluster.getPeerInfo()).isEmpty();
      // create cluster object from json return code
      Collection<DataCenterSpec> centers = cluster.getDataCenters();
      assertThat(centers.size()).isEqualTo(1);
      DataCenterSpec center = centers.iterator().next();
      assertThat(new Long(0)).isEqualTo(center.getId());
      assertThat(cluster).isEqualTo(center.getCluster());
      assertThat("dc1").isEqualTo(center.getName());
      Collection<NodeSpec> nodes = center.getNodes();
      assertThat(1).isEqualTo(nodes.size());
      NodeSpec node = nodes.iterator().next();
      assertThat(new Long(0)).isEqualTo(node.getId());
      assertThat("node1").isEqualTo(node.getName());
      assertThat(cluster).isEqualTo(node.getCluster());
      assertThat(center).isEqualTo(node.getDataCenter());
      assertThat(node.getAddress()).isNotNull();
    } catch (Exception e) {
      fail("Exception encountered");
    }
  }

  @Test
  public void testClusterCreationLarge() throws Exception {
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=3,3,3",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();

    HttpTestResponse responseToValidate = future.get();
    validateCluster(responseToValidate, 201);

    // create cluster object from json return code
    ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);

    client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future2 = new CompletableFuture<>();
    client
        .request(
            HttpMethod.GET,
            portNum,
            "127.0.0.1",
            "/cluster/" + cluster.getId(),
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future2.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();
    responseToValidate = future2.get();
    validateCluster(responseToValidate, 200);
  }

  @Test
  public void testUnregisterClusterNotExists() throws Exception {
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.DELETE,
            portNum,
            "127.0.0.1",
            "/cluster/0",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();

    HttpTestResponse response = future.get(5, TimeUnit.SECONDS);
    assertThat(response.response.statusCode()).isEqualTo(404);
    assertThat(response.response.getHeader("content-type")).isEqualTo("application/json");
    ErrorMessage error = om.readValue(response.body, ErrorMessage.class);
    assertThat(error.getMessage()).isEqualTo("No cluster registered with id or name 0.");
    assertThat(error.getStatusCode()).isEqualTo(404);
  }

  @Test
  public void testUnregisterClusterExists() throws Exception {
    unregisterClusterExists(BoundCluster::getName);
    unregisterClusterExists(p -> p.getId().toString());
  }

  private void unregisterClusterExists(Function<BoundCluster, String> f) throws Exception {
    BoundCluster cluster = nativeServer.register(ClusterSpec.builder().withNodes(1).build());
    BoundCluster cluster2 = nativeServer.register(ClusterSpec.builder().withNodes(1).build());

    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.DELETE,
            portNum,
            "127.0.0.1",
            "/cluster/" + f.apply(cluster),
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();

    HttpTestResponse response = future.get(5, TimeUnit.SECONDS);
    assertThat(response.response.statusCode()).isEqualTo(202);
    assertThat(response.response.getHeader("content-type")).isEqualTo("application/json");
    Message msg = om.readValue(response.body, Message.class);
    assertThat(msg.getMessage()).isEqualTo("Cluster " + f.apply(cluster) + " unregistered.");
    assertThat(msg.getStatusCode()).isEqualTo(202);

    // ClusterSpec should have been unregistered
    assertThat(nativeServer.getCluster(cluster.getId())).isNull();
    // Cluster2 should not have been unregistered
    assertThat(nativeServer.getCluster(cluster2.getId())).isNotNull();
  }

  @Test
  public void testUnregisterAllNoClusters() throws Exception {
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.DELETE,
            portNum,
            "127.0.0.1",
            "/cluster",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();

    HttpTestResponse response = future.get(5, TimeUnit.SECONDS);
    assertThat(response.response.statusCode()).isEqualTo(202);
    assertThat(response.response.getHeader("content-type")).isEqualTo("application/json");
    Message msg = om.readValue(response.body, Message.class);
    assertThat(msg.getMessage()).isEqualTo("All (0) clusters unregistered.");
    assertThat(msg.getStatusCode()).isEqualTo(202);
  }

  @Test
  public void testUnregisterAllWithClusters() throws Exception {
    // register 10 clusters and capture their ids.
    List<Long> ids = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      ids.add(nativeServer.register(ClusterSpec.builder().withNodes(1).build()).getId());
    }

    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.DELETE,
            portNum,
            "127.0.0.1",
            "/cluster",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                  });
            })
        .end();

    HttpTestResponse response = future.get(5, TimeUnit.SECONDS);
    assertThat(response.response.statusCode()).isEqualTo(202);
    assertThat(response.response.getHeader("content-type")).isEqualTo("application/json");
    Message msg = om.readValue(response.body, Message.class);
    assertThat(msg.getMessage()).isEqualTo("All (10) clusters unregistered.");
    assertThat(msg.getStatusCode()).isEqualTo(202);

    // Should be no more registered clusters
    assertThat(nativeServer.getClusters()).isEmpty();
  }

  @Test
  public void testClusterCreationByNameAndId() {
    testVerifyQueryParticularCluster(ClusterSpec::getName);
    testVerifyQueryParticularCluster(p -> p.getId().toString());
    testVerifyQueryParticularDatacenter(ClusterSpec::getName, DataCenterSpec::getName);
    testVerifyQueryParticularDatacenter(ClusterSpec::getName, p -> p.getId().toString());
    testVerifyQueryParticularNode(
        p -> p.getId().toString(), DataCenterSpec::getName, NodeSpec::getName);
    testVerifyQueryParticularNode(
        p -> p.getId().toString(), DataCenterSpec::getName, p -> p.getId().toString());
  }

  @Test
  public void testQueryClearTarget() {
    HttpClient client = vertx.createHttpClient();
    ClusterSpec cluster = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + cluster.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    List<DataCenterSpec> datacenters = (List<DataCenterSpec>) cluster.getDataCenters();
    DataCenterSpec datacenter = datacenters.get(0);

    this.primeSimpleRequest(client, prime, cluster.getName(), datacenter.getName());

    Iterator<NodeSpec> nodeIteratorQueried = cluster.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      NodeSpec node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      if (node.getDataCenter().equals(datacenter)) {
        assertThat(1).isEqualTo(set.all().size());
      } else {
        assertThat(0).isEqualTo(set.all().size());
      }
    }
    this.clearQueries(client, new Scope(cluster.getId(), datacenter.getId(), null));
    while (nodeIteratorQueried.hasNext()) {
      NodeSpec node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      assertThat(0).isEqualTo(set.all().size());
    }
  }

  @Test
  public void testClusterCreationWithNumberOfTokens() {
    int numTokens = 256;
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=1&num_tokens=" + numTokens,
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    String body = totalBuffer.toString();
                    HttpTestResponse testResponse = new HttpTestResponse(response, body);
                    future.complete(testResponse);
                  });
            })
        .end();

    try {
      HttpTestResponse responseToValidate = future.get();
      ObjectMapper om = ObjectMapperHolder.getMapper();
      // create cluster object from json return code
      ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      assertThat(new Long(0)).isEqualTo(cluster.getId());
      assertThat("0").isEqualTo(cluster.getName());
      assertThat(cluster.getPeerInfo()).isEmpty();
      // create cluster object from json return code
      Collection<DataCenterSpec> centers = cluster.getDataCenters();
      assertThat(centers.size()).isEqualTo(1);
      DataCenterSpec center = centers.iterator().next();
      assertThat(new Long(0)).isEqualTo(center.getId());
      assertThat(cluster).isEqualTo(center.getCluster());
      assertThat("dc1").isEqualTo(center.getName());
      Collection<NodeSpec> nodes = center.getNodes();
      assertThat(1).isEqualTo(nodes.size());
      NodeSpec node = nodes.iterator().next();
      assertThat(new Long(0)).isEqualTo(node.getId());
      assertThat("node1").isEqualTo(node.getName());
      assertThat(cluster).isEqualTo(node.getCluster());
      assertThat(center).isEqualTo(node.getDataCenter());
      assertThat(node.getAddress()).isNotNull();
      assertThat(node.getPeerInfo().containsKey("tokens")).isTrue();
      String tokenInfo = (String) node.getPeerInfo().get("tokens");
      String[] tokens = tokenInfo.split(",");
      assertThat(tokens.length).isEqualTo(numTokens);
      for (String token : tokens) {
        Long.parseLong(token);
      }
    } catch (Exception e) {
      fail("Exception encountered");
    }
  }

  @Test
  public void testClusterCreationWithNumberOfTokensDcs() {
    int numTokens = 256;
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=3,3,3&num_tokens=" + numTokens,
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    String body = totalBuffer.toString();
                    HttpTestResponse testResponse = new HttpTestResponse(response, body);
                    future.complete(testResponse);
                  });
            })
        .end();

    try {
      HttpTestResponse responseToValidate = future.get();
      ObjectMapper om = ObjectMapperHolder.getMapper();
      // create cluster object from json return code
      ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      assertThat(new Long(0)).isEqualTo(cluster.getId());
      assertThat("0").isEqualTo(cluster.getName());
      assertThat(cluster.getPeerInfo()).isEmpty();
      // create cluster object from json return code
      Collection<DataCenterSpec> centers = cluster.getDataCenters();
      assertThat(centers.size()).isEqualTo(3);
      long currentCenterId = 0;
      for (DataCenterSpec center : centers) {
        assertThat(currentCenterId++).isEqualTo(center.getId());
        assertThat(cluster).isEqualTo(center.getCluster());
        assertThat("dc" + currentCenterId).isEqualTo(center.getName());
        Collection<NodeSpec> nodes = center.getNodes();
        assertThat(3).isEqualTo(nodes.size());
        long currentId = 0;
        for (NodeSpec node : nodes) {
          assertThat(currentId++).isEqualTo(node.getId());
          assertThat("node" + currentId).isEqualTo(node.getName());
          assertThat(cluster).isEqualTo(node.getCluster());
          assertThat(center).isEqualTo(node.getDataCenter());
          assertThat(node.getAddress()).isNotNull();
          assertThat(node.getPeerInfo().containsKey("tokens")).isTrue();
          String tokenInfo = (String) node.getPeerInfo().get("tokens");
          String[] tokens = tokenInfo.split(",");
          assertThat(tokens.length).isEqualTo(numTokens);
          for (String token : tokens) {
            Long.parseLong(token);
          }
        }
      }
    } catch (Exception e) {
      fail("Exception encountered");
    }
  }

  private void testVerifyQueryParticularCluster(Function<ClusterSpec, String> f) {
    HttpClient client = vertx.createHttpClient();
    ClusterSpec clusterQueried = this.createMultiNodeCluster(client, "3,3");
    ClusterSpec clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    HttpTestResponse response =
        this.primeSimpleRequest(client, prime, "/prime" + "/" + f.apply(clusterQueried));

    Iterator<NodeSpec> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<NodeSpec> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      NodeSpec node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      List<Row> results = set.all();
      assertThat(1).isEqualTo(results.size());
    }

    while (nodeIteratorUnused.hasNext()) {
      String contactPointUnused =
          HttpTestUtil.getContactPointStringByNodeID(nodeIteratorUnused.next());
      ResultSet setUnused = HttpTestUtil.makeNativeQuery(query, contactPointUnused);
      List<Row> resultsUnused = setUnused.all();
      assertThat(0).isEqualTo(resultsUnused.size());
    }
  }

  private void testVerifyQueryParticularDatacenter(
      Function<ClusterSpec, String> fc, Function<DataCenterSpec, String> fd) {
    HttpClient client = vertx.createHttpClient();
    ClusterSpec clusterQueried = this.createMultiNodeCluster(client, "3,3");
    ClusterSpec clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    List<DataCenterSpec> datacenters = (List<DataCenterSpec>) clusterQueried.getDataCenters();
    DataCenterSpec datacenterQueried = datacenters.get(0);

    this.primeSimpleRequest(client, prime, fc.apply(clusterQueried), fd.apply(datacenterQueried));

    Iterator<NodeSpec> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<NodeSpec> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      NodeSpec node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      List<Row> results = set.all();
      if (node.getDataCenter().equals(datacenterQueried)) {
        assertThat(1).isEqualTo(results.size());
      } else {
        assertThat(0).isEqualTo(results.size());
      }
    }

    while (nodeIteratorUnused.hasNext()) {
      String contactPointUnused =
          HttpTestUtil.getContactPointStringByNodeID(nodeIteratorUnused.next());
      ResultSet setUnused = HttpTestUtil.makeNativeQuery(query, contactPointUnused);
      List<Row> resultsUnused = setUnused.all();
      assertThat(0).isEqualTo(resultsUnused.size());
    }
  }

  private void testVerifyQueryParticularNode(
      Function<ClusterSpec, String> fc,
      Function<DataCenterSpec, String> fd,
      Function<NodeSpec, String> fn) {
    HttpClient client = vertx.createHttpClient();
    ClusterSpec clusterQueried = this.createMultiNodeCluster(client, "3,3");
    ClusterSpec clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);

    List<DataCenterSpec> datacenters = (List<DataCenterSpec>) clusterQueried.getDataCenters();
    DataCenterSpec datacenterQueried = datacenters.get(0);
    Iterator<NodeSpec> datacenterIterator = datacenterQueried.getNodes().iterator();

    NodeSpec nodeQueried = datacenterIterator.next();

    this.primeSimpleRequest(
        client,
        prime,
        fc.apply(clusterQueried),
        fd.apply(datacenterQueried),
        fn.apply(nodeQueried));

    Iterator<NodeSpec> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<NodeSpec> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      NodeSpec node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      List<Row> results = set.all();
      if (node.equals(nodeQueried)) {
        assertThat(1).isEqualTo(results.size());
      } else {
        System.out.println(node + ", " + node.getCluster());
        assertThat(0).isEqualTo(results.size());
      }
    }

    while (nodeIteratorUnused.hasNext()) {
      String contactPointUnused =
          HttpTestUtil.getContactPointStringByNodeID(nodeIteratorUnused.next());
      ResultSet setUnused = HttpTestUtil.makeNativeQuery(query, contactPointUnused);
      List<Row> resultsUnused = setUnused.all();
      assertThat(0).isEqualTo(resultsUnused.size());
    }
  }

  private RequestPrime createSimplePrimedQuery(String query) {
    return createSimpleParameterizedQuery(query, null, null);
  }

  private RequestPrime createSimpleParameterizedQuery(
      String query, HashMap<String, Object> params, HashMap<String, String> paramTypes) {
    Query when = new Query(query, Collections.emptyList(), params, paramTypes);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    HashMap row1 = new HashMap<String, String>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    Map<String, String> column_types = new HashMap<String, String>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    Result then = new SuccessResult(rows, column_types);
    RequestPrime requestPrime = new RequestPrime(when, then);
    return requestPrime;
  }

  private HttpTestResponse primeSimpleRequest(HttpClient client, RequestPrime query, String path) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    try {
      String jsonPrime = om.writerWithDefaultPrettyPrinter().writeValueAsString(query);

      client
          .request(
              HttpMethod.POST,
              portNum,
              "127.0.0.1",
              path,
              response -> {
                response.bodyHandler(
                    totalBuffer -> {
                      String body = totalBuffer.toString();
                      HttpTestResponse testResponse = new HttpTestResponse(response, body);
                      future.complete(testResponse);
                    });
              })
          .putHeader("content-length", Integer.toString(jsonPrime.length()))
          .write(jsonPrime)
          .end();

      HttpTestResponse responseToValidate = future.get();
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      return responseToValidate;
    } catch (Exception e) {
      logger.error("Exception", e);
      fail("Exception encountered");
    }
    return null;
  }

  private HttpTestResponse primeSimpleRequest(
      HttpClient client, RequestPrime query, String ClusterID, String DatacenterID) {
    return this.primeSimpleRequest(client, query, "/prime" + "/" + ClusterID + "/" + DatacenterID);
  }

  private HttpTestResponse primeSimpleRequest(
      HttpClient client, RequestPrime query, String ClusterID, String DatacenterID, String nodeID) {
    return this.primeSimpleRequest(
        client, query, "/prime" + "/" + ClusterID + "/" + DatacenterID + "/" + nodeID);
  }

  private HttpTestResponse clearQueries(HttpClient client, Scope scope) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    try {
      client
          .request(
              HttpMethod.DELETE,
              portNum,
              "127.0.0.1",
              "/prime/" + scope.toString(),
              response -> {
                response.bodyHandler(
                    totalBuffer -> {
                      String body = totalBuffer.toString();
                      HttpTestResponse testResponse = new HttpTestResponse(response, body);
                      future.complete(testResponse);
                    });
              })
          .end();

      HttpTestResponse responseToValidate = future.get();
      assertThat(responseToValidate.response.statusCode()).isEqualTo(202);
      return responseToValidate;
    } catch (Exception e) {
      logger.error("Exception", e);
      fail("Exception encountered");
    }
    return null;
  }

  private HttpTestResponse clearQueries(HttpClient client) {
    return this.clearQueries(client, new Scope(null, null, null));
  }

  private ClusterSpec createSingleNodeCluster(HttpClient client) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=1",
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    String body = totalBuffer.toString();
                    HttpTestResponse testResponse = new HttpTestResponse(response, body);
                    future.complete(testResponse);
                  });
            })
        .end();

    try {
      HttpTestResponse responseToValidate = future.get();
      ObjectMapper om = ObjectMapperHolder.getMapper();
      // create cluster object from json return code
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
      return cluster;
    } catch (Exception e) {
      fail("Exception encountered");
      return null;
    }
  }

  private ClusterSpec createMultiNodeCluster(HttpClient client, String datacenters) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?data_centers=" + datacenters,
            response -> {
              response.bodyHandler(
                  totalBuffer -> {
                    String body = totalBuffer.toString();
                    HttpTestResponse testResponse = new HttpTestResponse(response, body);
                    future.complete(testResponse);
                  });
            })
        .end();

    try {
      HttpTestResponse responseToValidate = future.get();
      ObjectMapper om = ObjectMapperHolder.getMapper();
      // create cluster object from json return code
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
      return cluster;
    } catch (Exception e) {
      fail(e.getMessage());
      return null;
    }
  }

  private void validateCluster(HttpTestResponse responseToValidate, int expectedStatusCode)
      throws Exception {
    ObjectMapper om = ObjectMapperHolder.getMapper();
    // create cluster object from json return code
    ClusterSpec cluster = om.readValue(responseToValidate.body, ClusterSpec.class);
    assertThat(responseToValidate.response.statusCode()).isEqualTo(expectedStatusCode);
    assertThat(new Long(0)).isEqualTo(cluster.getId());
    assertThat("0").isEqualTo(cluster.getName());
    assertThat(cluster.getPeerInfo()).isEmpty();
    // create cluster object from json return code
    Collection<DataCenterSpec> centers = cluster.getDataCenters();
    assertThat(centers.size()).isEqualTo(3);
    for (DataCenterSpec center : centers.toArray(new DataCenterSpec[centers.size()])) {
      assertThat(center.getId()).isNotNull();
      assertThat(cluster.getName()).isNotNull();

      assertThat(cluster).isEqualTo(center.getCluster());
      Collection<NodeSpec> nodes = center.getNodes();
      assertThat(3).isEqualTo(nodes.size());
      for (NodeSpec node : nodes.toArray(new NodeSpec[nodes.size()])) {
        assertThat(node.getId()).isNotNull();
        assertThat(node.getName()).isNotNull();
        assertThat(cluster).isEqualTo(node.getCluster());
        assertThat(center).isEqualTo(node.getDataCenter());
      }
    }
  }
}
