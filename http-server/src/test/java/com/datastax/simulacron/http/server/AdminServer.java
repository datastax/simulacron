package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.simulacron.common.cluster.QueryPrime;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class AdminServer extends ExternalResource {

  private HttpContainer httpContainer;

  private Vertx vertx;

  private Server nativeServer;

  private static int port = 8187;

  private final Cluster clusterRequest;

  private Cluster cluster;

  private HttpClient client;

  private ObjectMapper om = ObjectMapperHolder.getMapper();

  Logger logger = LoggerFactory.getLogger(AdminServer.class);

  public AdminServer() {
    this(null);
  }

  public AdminServer(Cluster cluster) {
    this.clusterRequest = cluster;
  }

  @Override
  protected void before() throws Exception {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(port++, true);
    nativeServer = Server.builder().build();

    if (clusterRequest != null) {
      cluster = nativeServer.register(clusterRequest).get(10, TimeUnit.SECONDS);
    }

    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
    QueryManager qManager = new QueryManager(nativeServer);
    qManager.registerWithRouter(httpContainer.getRouter());
    httpContainer.start();
    client = vertx.createHttpClient();
  }

  @Override
  protected void after() {
    httpContainer.stop();
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.close(
        res -> {
          future.complete(null);
        });
    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Error encountered during cleanup", e);
      fail("Error encountered during cleanup");
    }

    try {
      nativeServer.unregisterAll().get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Error encountered unregistering native server clusters", e);
    }
  }

  public Cluster getCluster() {
    return cluster;
  }

  private HttpTestResponse request(HttpMethod method, String endpoint, String content)
      throws Exception {
    CompletableFuture<HttpTestResponse> future = new CompletableFuture<>();
    HttpClientRequest request =
        client
            .request(
                method,
                this.httpContainer.getPort(),
                this.httpContainer.getHost(),
                endpoint,
                response ->
                    response.bodyHandler(
                        totalBuffer -> {
                          String body = totalBuffer.toString();
                          HttpTestResponse testResponse = new HttpTestResponse(response, body);
                          future.complete(testResponse);
                        }))
            .putHeader("content-type", "application/json");

    if (content != null) {
      request.putHeader("content-length", Integer.toString(content.length())).write(content);
    }

    request.end();

    return future.get(10, TimeUnit.SECONDS);
  }

  public HttpTestResponse get(String endpoint) throws Exception {
    return request(HttpMethod.GET, endpoint, null);
  }

  public HttpTestResponse post(String endpoint, String content) throws Exception {
    return request(HttpMethod.POST, endpoint, content);
  }

  public HttpTestResponse delete(String endpoint) throws Exception {
    return request(HttpMethod.DELETE, endpoint, null);
  }

  public HttpTestResponse prime(QueryPrime prime) throws Exception {
    String jsonPrime = om.writerWithDefaultPrettyPrinter().writeValueAsString(prime);
    return post("/prime-query-single", jsonPrime);
  }
}
