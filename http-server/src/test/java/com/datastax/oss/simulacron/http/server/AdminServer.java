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

import static org.junit.Assert.fail;

import com.datastax.oss.simulacron.common.cluster.AbstractNodeProperties;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.BoundCluster;
import com.datastax.oss.simulacron.server.Server;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminServer extends ExternalResource {

  private HttpContainer httpContainer;

  private Vertx vertx;

  private Server nativeServer;

  private static int port = 8187;

  private final ClusterSpec clusterRequest;

  private BoundCluster cluster;

  private HttpClient client;

  private ObjectMapper om = ObjectMapperHolder.getMapper();

  private final Server.Builder serverBuilder;

  private static final Logger logger = LoggerFactory.getLogger(AdminServer.class);

  AdminServer(ClusterSpec cluster) {
    this(cluster, Server.builder());
  }

  AdminServer(ClusterSpec cluster, Server.Builder serverBuilder) {
    this.clusterRequest = cluster;
    this.serverBuilder = serverBuilder;
  }

  @Override
  protected void before() throws Exception {
    vertx = Vertx.vertx();
    httpContainer = new HttpContainer(port++, true);
    nativeServer = serverBuilder.build();

    if (clusterRequest != null) {
      cluster = nativeServer.register(clusterRequest);
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
    vertx.close(res -> future.complete(null));
    try {
      future.get(5, TimeUnit.SECONDS);
    } catch (Exception e) {
      logger.error("Error encountered during cleanup", e);
      fail("Error encountered during cleanup");
    }

    try {
      nativeServer.unregisterAll();
    } catch (Exception e) {
      logger.error("Error encountered unregistering native server clusters", e);
    }
  }

  BoundCluster getCluster() {
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

  HttpTestResponse get(String endpoint) throws Exception {
    return request(HttpMethod.GET, endpoint, null);
  }

  HttpTestResponse post(String endpoint, String content) throws Exception {
    return request(HttpMethod.POST, endpoint, content);
  }

  HttpTestResponse delete(String endpoint) throws Exception {
    return request(HttpMethod.DELETE, endpoint, null);
  }

  HttpTestResponse put(String endpoint) throws Exception {
    return request(HttpMethod.PUT, endpoint, null);
  }

  HttpTestResponse prime(PrimeDsl.PrimeBuilder primeBuilder) throws Exception {
    return prime(primeBuilder.build());
  }

  HttpTestResponse prime(Prime prime) throws Exception {
    return prime(prime.getPrimedRequest());
  }

  HttpTestResponse prime(RequestPrime prime) throws Exception {
    String jsonPrime = om.writerWithDefaultPrettyPrinter().writeValueAsString(prime);
    return post("/prime/" + cluster.getId(), jsonPrime);
  }

  <T> T mapTo(HttpTestResponse response, Class<T> clazz) throws IOException {
    return om.readValue(response.body, clazz);
  }

  public ClusterQueryLogReport getLogs(AbstractNodeProperties topic) throws Exception {
    String path = topic.resolveId();
    return om.readValue(get("/log/" + path).body, new TypeReference<ClusterQueryLogReport>() {});
  }

  public ClusterQueryLogReport getLogs(String path) throws Exception {
    return om.readValue(get("/log/" + path).body, new TypeReference<ClusterQueryLogReport>() {});
  }
}
