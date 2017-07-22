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
package com.datastax.oss.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.cluster.AbstractCluster;
import com.datastax.oss.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeConnectionReport;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.StubMapping;
import com.datastax.oss.simulacron.server.listener.QueryListener;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.channel.Channel;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A wrapper around {@link ClusterSpec} that is bound to a {@link Server}. If used as {@link
 * java.io.Closeable} will unbind itself form its bound server.
 */
public class BoundCluster extends AbstractCluster<BoundDataCenter, BoundNode>
    implements BoundTopic<ClusterConnectionReport, ClusterQueryLogReport> {

  private final transient Server server;

  private final transient StubStore stubStore;

  private final transient List<QueryListenerWrapper> queryListeners = new ArrayList<>();

  BoundCluster(ClusterSpec delegate, Long clusterId, Server server) {
    super(
        delegate.getName(),
        clusterId,
        delegate.getCassandraVersion(),
        delegate.getDSEVersion(),
        delegate.getPeerInfo());
    this.server = server;
    this.stubStore = new StubStore();
  }

  @Override
  public StubStore getStubStore() {
    return stubStore;
  }

  @Override
  public int clearPrimes(boolean nested) {
    int cleared = getStubStore().clear();
    if (nested) {
      for (BoundDataCenter dc : getDataCenters()) {
        cleared += dc.clearPrimes(true);
      }
    }
    return cleared;
  }

  @Override
  public CompletionStage<BoundCluster> unregisterAsync() {
    return getServer().unregisterAsync(this);
  }

  @Override
  public ClusterConnectionReport getConnections() {
    ClusterConnectionReport clusterConnectionReport = new ClusterConnectionReport(getId());
    for (BoundNode node : this.getNodes()) {
      clusterConnectionReport.addNode(
          node,
          node.clientChannelGroup.stream().map(Channel::remoteAddress).collect(Collectors.toList()),
          node.getAddress());
    }
    return clusterConnectionReport;
  }

  @Override
  public CompletionStage<ClusterConnectionReport> closeConnectionsAsync(CloseType closeType) {
    ClusterConnectionReport report = getConnections();
    return CompletableFuture.allOf(
            this.getNodes()
                .stream()
                .map(n -> n.closeConnectionsAsync(closeType).toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> report);
  }

  @Override
  public CompletionStage<ClusterConnectionReport> closeConnectionAsync(
      SocketAddress connection, CloseType type) {

    for (BoundNode node : this.getNodes()) {
      // identify the node that has the connection and close it with that node.
      for (SocketAddress address : node.getConnections().getConnections()) {
        if (connection.equals(address)) {
          return node.closeConnectionAsync(address, type)
              .thenApply(NodeConnectionReport::getRootReport);
        }
      }
    }

    CompletableFuture<ClusterConnectionReport> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IllegalArgumentException("Not found"));
    return failedFuture;
  }

  /**
   * Returns a QueryLogReport that contains all the logs for this cluster
   *
   * @return QueryLogReport
   */
  @Override
  @JsonIgnore
  public ClusterQueryLogReport getLogs() {
    ClusterQueryLogReport clusterQueryLogReport = new ClusterQueryLogReport(getId());
    this.getNodes().forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs()));
    return clusterQueryLogReport;
  }

  /**
   * Returns a QueryLogReport that contains filtered logs for this cluster
   *
   * @return QueryLogReport
   */
  @Override
  @JsonIgnore
  public ClusterQueryLogReport getLogs(boolean primed) {
    ClusterQueryLogReport clusterQueryLogReport = new ClusterQueryLogReport(getId());
    this.getNodes().forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs(primed)));
    return clusterQueryLogReport;
  }

  @Override
  public void registerQueryListener(
      QueryListener queryListener, boolean after, Predicate<QueryLog> filter) {
    queryListeners.add(new QueryListenerWrapper(queryListener, after, filter));
  }

  @Override
  public Server getServer() {
    return server;
  }

  Optional<StubMapping> find(BoundNode node, Frame frame) {
    Optional<StubMapping> stub = stubStore.find(node, frame);
    if (!stub.isPresent() && server != null) {
      stub = server.stubStore.find(node, frame);
    }
    return stub;
  }

  void notifyQueryListeners(BoundNode node, QueryLog queryLog, boolean after) {
    if (queryLog != null && !queryListeners.isEmpty()) {
      for (QueryListenerWrapper wrapper : queryListeners) {
        if (after == wrapper.after) {
          wrapper.apply(node, queryLog);
        }
      }
    }
  }
}
