package com.datastax.simulacron.http.server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.simulacron.common.cluster.*;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

public class HttpContainerTest {
  private HttpContainer httpContainer;
  private Vertx vertx = null;
  private int portNum = 8187;
  private Server nativeServer;
  ObjectMapper om = ObjectMapperHolder.getMapper();
  Logger logger = LoggerFactory.getLogger(HttpContainerTest.class);

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(portNum, true);
    nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
    QueryManager qManager = new QueryManager(nativeServer);
    qManager.registerWithRouter(httpContainer.getRouter());
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
      assertEquals(responseToValidate.response.statusCode(), 201);
      assertEquals(new Long(0), cluster.getId());
      assertEquals("0", cluster.getName());
      assertNotNull(cluster.getPeerInfo());
      //create cluster object from json return code
      Collection<DataCenter> centers = cluster.getDataCenters();
      assertEquals(centers.size(), 1);
      DataCenter center = centers.iterator().next();
      assertEquals(new Long(0), center.getId());
      assertEquals(cluster, center.getCluster());
      assertEquals("dc1", center.getName());
      Collection<Node> nodes = center.getNodes();
      assertEquals(1, nodes.size());
      Node node = nodes.iterator().next();
      assertEquals(new Long(0), node.getId());
      assertEquals("node1", node.getName());
      assertEquals(cluster, node.getCluster());
      assertEquals(center, node.getDataCenter());
      assertNotNull(node.getAddress());

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
    assertThat(error.getMessage()).isEqualTo("No cluster registered with id 0.");
    assertThat(error.getStatusCode()).isEqualTo(404);
  }

  @Test
  public void testUnregisterClusterExists() throws Exception {
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
            "/cluster/" + cluster.getId(),
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
    assertThat(msg.getMessage()).isEqualTo("Cluster 0 unregistered.");
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
  public void testQueryPrimeSimple() {
    try {
      HttpClient client = vertx.createHttpClient();
      Cluster clusterCreated = this.createSingleNodeCluster(client);

      QueryPrime prime = createSimplePrimedQuery("Select * FROM TABLE2");
      HttpTestResponse response = this.primeSimpleQuery(client, prime);
      assertNotNull(response);
      QueryPrime responseQuery = om.readValue(response.body, QueryPrime.class);
      assertEquals(responseQuery, prime);

      String contactPoint = getContactPointString(clusterCreated);
      ResultSet set = makeNativeQuery("Select * FROM TABLE2", contactPoint);
      List<Row> results = set.all();
      assertEquals(1, results.size());
      Row row1 = results.get(0);
      String column1 = row1.getString("column1");
      assertEquals(column1, "column1");
      Long column2 = row1.getLong("column2");
      assertEquals(column2, new Long(2));
    } catch (Exception e) {
      fail("error encountered");
    }
  }

  private String getContactPointString(Cluster cluster) {
    String rawaddress = cluster.getNodes().get(0).getAddress().toString();
    return rawaddress.substring(1, rawaddress.length() - 5);
  }

  private ResultSet makeNativeQuery(String query, String contactPoint) {

    com.datastax.driver.core.Cluster cluster =
        com.datastax.driver.core.Cluster.builder().addContactPoint(contactPoint).build();

    Session session = cluster.connect();
    ResultSet set = session.execute(query);
    cluster.close();
    return set;
  }

  private QueryPrime createSimplePrimedQuery(String query) {
    QueryPrime.When when = new QueryPrime.When(query, null, null, null);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    Map<String, Object> row1 = new HashMap<>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    Map<String, String> column_types = new HashMap<String, String>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    SuccessResult then = new SuccessResult(rows, column_types);
    return new QueryPrime(when, then);
  }

  private HttpTestResponse primeSimpleQuery(HttpClient client, QueryPrime query) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    try {
      String jsonPrime = om.writerWithDefaultPrettyPrinter().writeValueAsString(query);

      client
          .request(
              HttpMethod.POST,
              portNum,
              "127.0.0.1",
              "/prime-query-single",
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
      assertEquals(responseToValidate.response.statusCode(), 201);
      return responseToValidate;
    } catch (Exception e) {
      logger.error("Exception", e);
      fail("Exception encountered");
    }
    return null;
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
      assertEquals(responseToValidate.response.statusCode(), 201);
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
    assertEquals(responseToValidate.response.statusCode(), expectedStatusCode);
    assertEquals(new Long(0), cluster.getId());
    assertEquals("0", cluster.getName());
    assertNotNull(cluster.getPeerInfo());
    //create cluster object from json return code
    Collection<DataCenter> centers = cluster.getDataCenters();
    assertEquals(centers.size(), 3);
    for (DataCenter center : centers.toArray(new DataCenter[centers.size()])) {
      assertNotNull(center.getId());
      assertNotNull(center.getName());
      assertEquals(cluster, center.getCluster());
      Collection<Node> nodes = center.getNodes();
      assertEquals(3, nodes.size());
      for (Node node : nodes.toArray(new Node[nodes.size()])) {
        assertNotNull(node.getId());
        assertNotNull(node.getName());
        assertEquals(cluster, node.getCluster());
        assertEquals(center, node.getDataCenter());
      }
    }
  }
}
