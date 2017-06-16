package com.datastax.simulacron.http.server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.result.Result;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.fail;

public class HttpContainerIntegrationTest {
  private HttpContainer httpContainer;
  private Vertx vertx = null;
  private int portNum = 8187;
  private Server nativeServer;
  ObjectMapper om = ObjectMapperHolder.getMapper();
  Logger logger = LoggerFactory.getLogger(HttpContainerIntegrationTest.class);

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(portNum, true);
    nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
    QueryManager qManager = new QueryManager(nativeServer);
    qManager.registerWithRouter(httpContainer.getRouter());
    ActivityLogManager logManager = new ActivityLogManager(nativeServer);
    logManager.registerWithRouter(httpContainer.getRouter());
    httpContainer.start();
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
      nativeServer.unregisterAll().get(5, TimeUnit.SECONDS);
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
      //create cluster object from json return code
      Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      assertThat(new Long(0)).isEqualTo(cluster.getId());
      assertThat("0").isEqualTo(cluster.getName());
      assertThat(cluster.getPeerInfo()).isEmpty();
      //create cluster object from json return code
      Collection<DataCenter> centers = cluster.getDataCenters();
      assertThat(centers.size()).isEqualTo(1);
      DataCenter center = centers.iterator().next();
      assertThat(new Long(0)).isEqualTo(center.getId());
      assertThat(cluster).isEqualTo(center.getCluster());
      assertThat("dc1").isEqualTo(center.getName());
      Collection<Node> nodes = center.getNodes();
      assertThat(1).isEqualTo(nodes.size());
      Node node = nodes.iterator().next();
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

    //create cluster object from json return code
    Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);

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
    unregisterClusterExists(Cluster::getName);
    unregisterClusterExists(p -> p.getId().toString());
  }

  private void unregisterClusterExists(Function<Cluster, String> f) throws Exception {
    Cluster cluster =
        nativeServer.register(Cluster.builder().withNodes(1).build()).get(1, TimeUnit.SECONDS);
    Cluster cluster2 =
        nativeServer.register(Cluster.builder().withNodes(1).build()).get(1, TimeUnit.SECONDS);

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

    // Cluster should have been unregistered
    assertThat(nativeServer.getClusterRegistry()).doesNotContainKey(cluster.getId());
    // Cluster2 should not have been unregistered
    assertThat(nativeServer.getClusterRegistry()).containsKey(cluster2.getId());
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
      ids.add(
          nativeServer
              .register(Cluster.builder().withNodes(1).build())
              .get(5, TimeUnit.SECONDS)
              .getId());
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
    assertThat(nativeServer.getClusterRegistry()).isEmpty();
  }

  @Test
  public void testClusterCreationByNameAndId() {
    testVerifyQueryParticularCluster(Cluster::getName);
    testVerifyQueryParticularCluster(p -> p.getId().toString());
    testVerifyQueryParticularDatacenter(Cluster::getName, DataCenter::getName);
    testVerifyQueryParticularDatacenter(Cluster::getName, p -> p.getId().toString());
    testVerifyQueryParticularNode(p -> p.getId().toString(), DataCenter::getName, Node::getName);
    testVerifyQueryParticularNode(
        p -> p.getId().toString(), DataCenter::getName, p -> p.getId().toString());
  }

  @Test
  public void testQueryClearAll() {
    try {
      HttpClient client = vertx.createHttpClient();
      Cluster clusterCreated = this.createSingleNodeCluster(client);

      RequestPrime prime = createSimplePrimedQuery("Select * FROM TABLE2");
      this.primeSimpleRequest(client, prime);
      String contactPoint = HttpTestUtil.getContactPointString(clusterCreated);
      ResultSet set = HttpTestUtil.makeNativeQuery("Select * FROM TABLE2", contactPoint);
      assertThat(1).isEqualTo(set.all().size());

      this.clearQueries(client);
      set = HttpTestUtil.makeNativeQuery("Select * FROM TABLE2", contactPoint);
      assertThat(0).isEqualTo(set.all().size());

    } catch (Exception e) {
      fail("error encountered");
    }
  }

  @Test
  public void testQueryClearTarget() {
    HttpClient client = vertx.createHttpClient();
    Cluster cluster = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + cluster.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    List<DataCenter> datacenters = (List<DataCenter>) cluster.getDataCenters();
    DataCenter datacenter = datacenters.get(0);

    this.primeSimpleRequest(client, prime, cluster.getName(), datacenter.getName());

    Iterator<Node> nodeIteratorQueried = cluster.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      Node node = nodeIteratorQueried.next();

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
      Node node = nodeIteratorQueried.next();

      String contactPoint = HttpTestUtil.getContactPointStringByNodeID(node);
      ResultSet set = HttpTestUtil.makeNativeQuery(query, contactPoint);
      assertThat(0).isEqualTo(set.all().size());
    }
  }

  private void testVerifyQueryParticularCluster(Function<Cluster, String> f) {
    HttpClient client = vertx.createHttpClient();
    Cluster clusterQueried = this.createMultiNodeCluster(client, "3,3");
    Cluster clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    HttpTestResponse response =
        this.primeSimpleRequest(
            client, prime, "/prime-query-single" + "/" + f.apply(clusterQueried));

    Iterator<Node> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<Node> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      Node node = nodeIteratorQueried.next();

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
      Function<Cluster, String> fc, Function<DataCenter, String> fd) {
    HttpClient client = vertx.createHttpClient();
    Cluster clusterQueried = this.createMultiNodeCluster(client, "3,3");
    Cluster clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);
    List<DataCenter> datacenters = (List<DataCenter>) clusterQueried.getDataCenters();
    DataCenter datacenterQueried = datacenters.get(0);

    this.primeSimpleRequest(client, prime, fc.apply(clusterQueried), fd.apply(datacenterQueried));

    Iterator<Node> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<Node> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      Node node = nodeIteratorQueried.next();

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
      Function<Cluster, String> fc, Function<DataCenter, String> fd, Function<Node, String> fn) {
    HttpClient client = vertx.createHttpClient();
    Cluster clusterQueried = this.createMultiNodeCluster(client, "3,3");
    Cluster clusterUnused = this.createMultiNodeCluster(client, "3,3");

    String query = "Select * FROM TABLE2_" + clusterQueried.getName();

    RequestPrime prime = createSimplePrimedQuery(query);

    List<DataCenter> datacenters = (List<DataCenter>) clusterQueried.getDataCenters();
    DataCenter datacenterQueried = datacenters.get(0);
    Iterator<Node> datacenterIterator = datacenterQueried.getNodes().iterator();

    Node nodeQueried = datacenterIterator.next();

    this.primeSimpleRequest(
        client,
        prime,
        fc.apply(clusterQueried),
        fd.apply(datacenterQueried),
        fn.apply(nodeQueried));

    Iterator<Node> nodeIteratorQueried = clusterQueried.getNodes().iterator();
    Iterator<Node> nodeIteratorUnused = clusterUnused.getNodes().iterator();

    while (nodeIteratorQueried.hasNext()) {
      Node node = nodeIteratorQueried.next();

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

  private HttpTestResponse primeSimpleRequest(HttpClient client, RequestPrime query) {
    return this.primeSimpleRequest(client, query, "/prime-query-single");
  }

  private HttpTestResponse primeSimpleRequest(
      HttpClient client, RequestPrime query, String ClusterID, String DatacenterID) {
    return this.primeSimpleRequest(
        client, query, "/prime-query-single" + "/" + ClusterID + "/" + DatacenterID);
  }

  private HttpTestResponse primeSimpleRequest(
      HttpClient client, RequestPrime query, String ClusterID, String DatacenterID, String nodeID) {
    return this.primeSimpleRequest(
        client, query, "/prime-query-single" + "/" + ClusterID + "/" + DatacenterID + "/" + nodeID);
  }

  private HttpTestResponse clearQueries(HttpClient client, Scope scope) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    try {
      client
          .request(
              HttpMethod.DELETE,
              portNum,
              "127.0.0.1",
              "/prime-query-single/" + scope.toString(),
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

  private Cluster createSingleNodeCluster(HttpClient client) {
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
      //create cluster object from json return code
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
      return cluster;
    } catch (Exception e) {
      fail("Exception encountered");
      return null;
    }
  }

  private Cluster createMultiNodeCluster(HttpClient client, String datacenters) {
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
      //create cluster object from json return code
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
      Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
      return cluster;
    } catch (Exception e) {
      fail("Exception encountered");
      return null;
    }
  }

  private void validateCluster(HttpTestResponse responseToValidate, int expectedStatusCode)
      throws Exception {
    ObjectMapper om = ObjectMapperHolder.getMapper();
    //create cluster object from json return code
    Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
    assertThat(responseToValidate.response.statusCode()).isEqualTo(expectedStatusCode);
    assertThat(new Long(0)).isEqualTo(cluster.getId());
    assertThat("0").isEqualTo(cluster.getName());
    assertThat(cluster.getPeerInfo()).isEmpty();
    //create cluster object from json return code
    Collection<DataCenter> centers = cluster.getDataCenters();
    assertThat(centers.size()).isEqualTo(3);
    for (DataCenter center : centers.toArray(new DataCenter[centers.size()])) {
      assertThat(center.getId()).isNotNull();
      assertThat(cluster.getName()).isNotNull();

      assertThat(cluster).isEqualTo(center.getCluster());
      Collection<Node> nodes = center.getNodes();
      assertThat(3).isEqualTo(nodes.size());
      for (Node node : nodes.toArray(new Node[nodes.size()])) {
        assertThat(node.getId()).isNotNull();
        assertThat(node.getName()).isNotNull();
        assertThat(cluster).isEqualTo(node.getCluster());
        assertThat(center).isEqualTo(node.getDataCenter());
      }
    }
  }
}
