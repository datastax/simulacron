package com.datastax.simulacron.http.server;

import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.PrimeDsl;
import com.datastax.simulacron.server.BoundCluster;
import com.datastax.simulacron.server.Server;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class AdminServer extends ExternalResource {

  private HttpContainer httpContainer;

  private Vertx vertx;

  private Server nativeServer;

  private static int port = 8187;

  private final Cluster clusterRequest;

  private BoundCluster cluster;

  private HttpClient client;

  private ObjectMapper om = ObjectMapperHolder.getMapper();

  private final Server.Builder serverBuilder;

  Logger logger = LoggerFactory.getLogger(AdminServer.class);

  public AdminServer() {
    this(null);
  }

  public AdminServer(Cluster cluster) {
    this(cluster, Server.builder());
  }

  public AdminServer(Cluster cluster, Server.Builder serverBuilder) {
    this.clusterRequest = cluster;
    this.serverBuilder = serverBuilder;
  }

  @Override
  protected void before() throws Exception {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(port++, true);
    nativeServer = serverBuilder.build();

    if (clusterRequest != null) {
      cluster = nativeServer.register(clusterRequest).get(10, TimeUnit.SECONDS);
    }

    ClusterManager provisioner = new ClusterManager(nativeServer);
    provisioner.registerWithRouter(httpContainer.getRouter());
    QueryManager qManager = new QueryManager(nativeServer);
    qManager.registerWithRouter(httpContainer.getRouter());
    ActivityLogManager logManager = new ActivityLogManager(nativeServer);
    logManager.registerWithRouter(httpContainer.getRouter());
    EndpointManager endpointManager = new EndpointManager(nativeServer);
    endpointManager.registerWithRouter(httpContainer.getRouter());
    httpContainer.start().get(10, TimeUnit.SECONDS);
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

  public BoundCluster getCluster() {
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

  public HttpTestResponse put(String endpoint) throws Exception {
    return request(HttpMethod.PUT, endpoint, null);
  }

  public HttpTestResponse prime(PrimeDsl.PrimeBuilder primeBuilder) throws Exception {
    return prime(primeBuilder.build());
  }

  public HttpTestResponse prime(Prime prime) throws Exception {
    return prime(prime.getPrimedRequest());
  }

  public HttpTestResponse prime(RequestPrime prime) throws Exception {
    String jsonPrime = om.writerWithDefaultPrettyPrinter().writeValueAsString(prime);
    return post("/prime/" + cluster.getId(), jsonPrime);
  }

  public <T> T mapTo(HttpTestResponse response, Class<T> clazz) throws IOException {
    return om.readValue(response.body, clazz);
  }

  public List<QueryLog> getLogs(AbstractNodeProperties topic) throws Exception {
    String path = topic.resolveId();
    return om.readValue(get("/log/" + path).body, new TypeReference<List<QueryLog>>() {});
  }

  public List<QueryLog> getLogs(String path) throws Exception {
    return om.readValue(get("/log/" + path).body, new TypeReference<List<QueryLog>>() {});
  }

  public ObjectMapper getObjectMapper() {
    return om;
  }
}
