package com.datastax.simulacron.http.server;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.common.result.Result;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
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
      QueryPrime responseQuery = (QueryPrime) om.readValue(response.body, QueryPrime.class);
      assertThat(responseQuery).isEqualTo(prime);

      String contactPoint = getContactPointString(clusterCreated);
      ResultSet set = makeNativeQuery("Select * FROM TABLE2", contactPoint);
      List<Row> results = set.all();
      assertThat(1).isEqualTo(results.size());
      Row row1 = results.get(0);
      String column1 = row1.getString("column1");
      assertThat(column1).isEqualTo("column1");
      Long column2 = row1.getLong("column2");
      assertThat(column2).isEqualTo(new Long(2));
    } catch (Exception e) {
      fail("error encountered");
    }
  }

  @Test
  public void testVerifyQueriesiFromCluster() {
    try {
      HttpClient client = vertx.createHttpClient();
      Cluster clusterCreated = this.createSingleNodeCluster(client);

      createSimplePrimedQuery("Select * FROM TABLE1");
      createSimplePrimedQuery("Select * FROM TABLE2");

      String contactPoint = getContactPointString(clusterCreated);
      makeNativeQuery("Select * FROM TABLE1", contactPoint);
      makeNativeQuery("Select * FROM TABLE2", contactPoint);

      HttpTestResponse queryLogResponse = getQueryLog(client, clusterCreated.getId().toString());
      assertThat(queryLogResponse.body).isNotEmpty();
      QueryLog[] queryLogs = om.readValue(queryLogResponse.body, QueryLog[].class);

      assertThat(queryLogs).isNotNull();
      assertThat(queryLogs.length).isGreaterThan(2);
    } catch (Exception e) {
      fail("error encountered");
    }
  }

  @Test
  public void testVerifyQueriesFromDataCenterAndNode() {
    try {
      HttpClient client = vertx.createHttpClient();
      Cluster clusterCreated = this.createMultiNodeCluster(client, "3,3");

      createSimplePrimedQuery("Select * FROM TABLE1");
      createSimplePrimedQuery("Select * FROM TABLE2");

      String contactPoint = getContactPointString(clusterCreated);
      makeNativeQuery("Select * FROM TABLE1", contactPoint);
      makeNativeQuery("Select * FROM TABLE2", contactPoint);

      HttpTestResponse queryLogResponse = getQueryLog(client, clusterCreated.getId() + "/0/0");
      assertThat(queryLogResponse.body).isNotEmpty();
      QueryLog[] queryLogs = om.readValue(queryLogResponse.body, QueryLog[].class);
      assertThat(queryLogs).isNotNull();
      assertThat(queryLogs.length).isGreaterThanOrEqualTo(2);

      queryLogResponse = getQueryLog(client, clusterCreated.getId() + "/1");
      assertThat(queryLogResponse.body).isNotEmpty();
      queryLogs = om.readValue(queryLogResponse.body, QueryLog[].class);
      assertThat(queryLogs).isNotNull();
      assertThat(queryLogs.length).isEqualTo(0);
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
    QueryPrime.When when = new QueryPrime.When(query, null);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    HashMap row1 = new HashMap<String, String>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    String result = "success";
    Map<String, String> column_types = new HashMap<String, String>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    Result then = new SuccessResult(rows, column_types);
    QueryPrime queryPrime = new QueryPrime(when, then);
    return queryPrime;
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
      assertThat(responseToValidate.response.statusCode()).isEqualTo(201);
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

  private HttpTestResponse getQueryLog(HttpClient client, String logPath) {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    try {
      client
          .request(
              HttpMethod.GET,
              portNum,
              "127.0.0.1",
              "/log/" + logPath,
              response -> {
                response.bodyHandler(
                    totalBuffer -> {
                      future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                    });
              })
          .end();

      HttpTestResponse responseToValidate = future.get();
      assertThat(responseToValidate.response.statusCode()).isEqualTo(200);
      return responseToValidate;
    } catch (Exception e) {
      logger.error("Exception", e);
      fail("Exception encountered");
    }
    return null;
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
