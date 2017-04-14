package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ClusterMapper;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

public class HttpContainerTest {
  private HttpContainer httpContainer;
  private Vertx vertx = null;
  private int portNum = 8187;
  Logger logger = LoggerFactory.getLogger(HttpContainerTest.class);

  @Before
  public void setup() {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(portNum, true);
    Server nativeServer = Server.builder().build();
    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
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
      logger.error("Error encountered httpcontainertest cleanup", e);
    }
    portNum++;
  }

  @Test
  public void testClutterCreationSimple() {
    HttpClient client = vertx.createHttpClient();
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    client
        .request(
            HttpMethod.POST,
            portNum,
            "127.0.0.1",
            "/cluster/?dataCenters=1",
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
      ObjectMapper om = ClusterMapper.getMapper();
      //create cluster object from json return code
      Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
      assertEquals(responseToValidate.response.statusCode(), 200);
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
  public void testClutterCreationLarge() {
    try {
      HttpClient client = vertx.createHttpClient();
      CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
      client
          .request(
              HttpMethod.POST,
              portNum,
              "127.0.0.1",
              "/cluster/?dataCenters=3,3,3",
              response -> {
                response.bodyHandler(
                    totalBuffer -> {
                      future.complete(new HttpTestResponse(response, totalBuffer.toString()));
                    });
              })
          .end();

      HttpTestResponse responseToValidate = future.get();
      validateCluster(responseToValidate);

      client = vertx.createHttpClient();
      CompletableFuture<HttpTestResponse> future2 = new CompletableFuture<>();
      client
          .request(
              HttpMethod.GET,
              portNum,
              "127.0.0.1",
              "/cluster/",
              response -> {
                response.bodyHandler(
                    totalBuffer -> {
                      future2.complete(new HttpTestResponse(response, totalBuffer.toString()));
                    });
              })
          .end();
      responseToValidate = future.get();
      validateCluster(responseToValidate);
    } catch (Exception e) {
      fail("Exception encountered");
    }
  }

  private void validateCluster(HttpTestResponse responseToValidate) throws Exception {
    ObjectMapper om = ClusterMapper.getMapper();
    //create cluster object from json return code
    Cluster cluster = om.readValue(responseToValidate.body, Cluster.class);
    assertEquals(responseToValidate.response.statusCode(), 200);
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
