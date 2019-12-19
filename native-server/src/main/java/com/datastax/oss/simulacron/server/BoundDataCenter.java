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

import static com.datastax.oss.simulacron.server.FrameCodecUtils.buildFrameCodec;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.cluster.AbstractDataCenter;
import com.datastax.oss.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.DataCenterConnectionReport;
import com.datastax.oss.simulacron.common.cluster.DataCenterQueryLogReport;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
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

public class BoundDataCenter extends AbstractDataCenter<BoundCluster, BoundNode>
    implements BoundTopic<DataCenterConnectionReport, DataCenterQueryLogReport> {

  private final transient Server server;

  private final transient BoundCluster cluster;

  private final transient StubStore stubStore;

  private final transient List<QueryListenerWrapper> queryListeners = new ArrayList<>();

  private final transient FrameCodecWrapper frameCodec;

  BoundDataCenter(BoundCluster parent) {
    super(
        "dummy",
        0L,
        parent.getCassandraVersion(),
        parent.getDSEVersion(),
        parent.getPeerInfo(),
        parent);
    this.server = parent.getServer();
    this.cluster = parent;
    this.stubStore = new StubStore();
    this.frameCodec = parent.getFrameCodec();
  }

  BoundDataCenter(DataCenterSpec delegate, BoundCluster parent) {
    super(
        delegate.getName(),
        delegate.getId(),
        delegate.getCassandraVersion(),
        delegate.getDSEVersion(),
        delegate.getPeerInfo(),
        parent);
    this.server = parent.getServer();
    this.cluster = parent;
    this.stubStore = new StubStore();
    this.frameCodec = buildFrameCodec(delegate).orElse(parent.getFrameCodec());
  }

  @Override
  public StubStore getStubStore() {
    return stubStore;
  }

  @Override
  public int clearPrimes(boolean nested) {
    int cleared = getStubStore().clear();
    if (nested) {
      for (BoundNode node : getNodes()) {
        cleared += node.clearPrimes(true);
      }
    }
    return cleared;
  }

  @Override
  public CompletionStage<BoundCluster> unregisterAsync() {
    return getServer().unregisterAsync(getCluster());
  }

  @Override
  public DataCenterConnectionReport getConnections() {
    ClusterConnectionReport clusterConnectionReport = new ClusterConnectionReport(cluster.getId());
    for (BoundNode node : this.getNodes()) {
      clusterConnectionReport.addNode(
          node,
          node.clientChannelGroup.stream().map(Channel::remoteAddress).collect(Collectors.toList()),
          node.getAddress());
    }
    return clusterConnectionReport.getDataCenters().iterator().next();
  }

  @Override
  public CompletionStage<DataCenterConnectionReport> closeConnectionsAsync(CloseType closeType) {
    DataCenterConnectionReport report = getConnections();
    return CompletableFuture.allOf(
            this.getNodes()
                .stream()
                .map(n -> n.closeConnectionsAsync(closeType).toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> report);
  }

  @Override
  public CompletionStage<DataCenterConnectionReport> closeConnectionAsync(
      SocketAddress connection, CloseType type) {

    for (BoundNode node : this.getNodes()) {
      // identify the node that has the connection and close it with that node.
      for (SocketAddress address : node.getConnections().getConnections()) {
        if (connection.equals(address)) {
          return node.closeConnectionAsync(address, type)
              .thenApply(n -> n.getRootReport().getDataCenters().iterator().next());
        }
      }
    }

    CompletableFuture<DataCenterConnectionReport> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IllegalArgumentException("Not found"));
    return failedFuture;
  }

  @Override
  public DataCenterConnectionReport pauseRead() {
    this.getNodes().forEach(BoundNode::pauseRead);
    return getConnections();
  }

  @Override
  public DataCenterConnectionReport resumeRead() {
    this.getNodes().forEach(BoundNode::resumeRead);
    return getConnections();
  }

  /**
   * Returns a QueryLogReport that contains filtered logs for this datacenter
   *
   * @return QueryLogReport
   */
  @Override
  @JsonIgnore
  public DataCenterQueryLogReport getLogs(boolean primed) {
    ClusterQueryLogReport clusterQueryLogReport = new ClusterQueryLogReport(cluster.getId());
    this.getNodes().forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs(primed)));
    return clusterQueryLogReport.getDataCenters().iterator().next();
  }

  @Override
  public void registerQueryListener(
      QueryListener queryListener, boolean after, Predicate<QueryLog> filter) {
    this.queryListeners.add(new QueryListenerWrapper(queryListener, after, filter));
  }

  /**
   * Returns a QueryLogReport that contains all the logs for this datacenter
   *
   * @return QueryLogReport
   */
  @Override
  @JsonIgnore
  public DataCenterQueryLogReport getLogs() {
    ClusterQueryLogReport clusterQueryLogReport = new ClusterQueryLogReport(cluster.getId());
    this.getNodes().forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs()));
    return clusterQueryLogReport.getDataCenters().iterator().next();
  }

  @Override
  public Server getServer() {
    return server;
  }

  @Override
  @JsonIgnore
  public FrameCodecWrapper getFrameCodec() {
    return frameCodec;
  }

  Optional<StubMapping> find(BoundNode node, Frame frame) {
    Optional<StubMapping> stub = stubStore.find(node, frame);
    if (!stub.isPresent()) {
      stub = getCluster().find(node, frame);
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
    getCluster().notifyQueryListeners(node, queryLog, after);
  }
}
