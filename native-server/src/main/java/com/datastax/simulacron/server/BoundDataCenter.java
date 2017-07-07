package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.*;
import com.datastax.simulacron.common.stubbing.CloseType;
import com.datastax.simulacron.common.stubbing.StubMapping;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.netty.channel.Channel;

import java.net.SocketAddress;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class BoundDataCenter extends DataCenter implements BoundTopic<DataCenterConnectionReport> {

  private final transient Server server;

  private final transient BoundCluster cluster;

  private final transient StubStore stubStore;

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
  }

  BoundDataCenter(DataCenter delegate, BoundCluster parent) {
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
  }

  /**
   * Convenience method to look up node by id.
   *
   * @param id The id of the node.
   * @return The node if found or null.
   */
  public BoundNode node(long id) {
    return this.getNodes()
        .stream()
        .filter(n -> n.getId() == id)
        .findFirst()
        .map(n -> (BoundNode) n)
        .orElse(null);
  }

  @Override
  public StubStore getStubStore() {
    return stubStore;
  }

  @Override
  public DataCenterConnectionReport getConnections() {
    ClusterConnectionReport clusterConnectionReport = new ClusterConnectionReport(cluster.getId());
    for (Node node : this.getNodes()) {
      BoundNode boundNode = (BoundNode) node;
      clusterConnectionReport.addNode(
          boundNode,
          boundNode
              .clientChannelGroup
              .stream()
              .map(Channel::remoteAddress)
              .collect(Collectors.toList()),
          boundNode.getAddress());
    }
    return clusterConnectionReport.getDataCenters().iterator().next();
  }

  @Override
  public CompletionStage<DataCenterConnectionReport> closeConnectionsAsync(CloseType closeType) {
    DataCenterConnectionReport report = getConnections();
    return CompletableFuture.allOf(
            this.getNodes()
                .stream()
                .map(n -> ((BoundNode) n).closeConnectionsAsync(closeType).toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> report);
  }

  @Override
  public CompletionStage<DataCenterConnectionReport> closeConnectionAsync(
      SocketAddress connection, CloseType type) {

    for (Node node : this.getNodes()) {
      BoundNode boundNode = (BoundNode) node;
      // identify the node that has the connection and close it with that node.
      for (SocketAddress address : boundNode.getConnections().getConnections()) {
        if (connection.equals(address)) {
          return boundNode
              .closeConnectionAsync(address, type)
              .thenApply(n -> n.getRootReport().getDataCenters().iterator().next());
        }
      }
    }

    CompletableFuture<DataCenterConnectionReport> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IllegalArgumentException("Not found"));
    return failedFuture;
  }

  @Override
  public Stream<BoundNode> getBoundNodes() {
    return getNodes().stream().map(n -> (BoundNode) n);
  }

  @Override
  @JsonIgnore
  public List<QueryLog> getLogs() {
    return getBoundNodes().flatMap(n -> n.getLogs().stream()).collect(Collectors.toList());
  }

  @Override
  public void clearLogs() {
    getBoundNodes().forEach(BoundNode::clearLogs);
  }

  @Override
  public BoundCluster getBoundCluster() {
    return cluster;
  }

  @Override
  public Server getServer() {
    return server;
  }

  Optional<StubMapping> find(Node node, Frame frame) {
    Optional<StubMapping> stub = stubStore.find(node, frame);
    if (!stub.isPresent()) {
      stub = getBoundCluster().find(node, frame);
    }
    return stub;
  }
}
