package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.ClusterConnectionReport;
import com.datastax.simulacron.common.cluster.ClusterQueryLogReport;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.cluster.NodeConnectionReport;
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

/**
 * A wrapper around {@link Cluster} that is bound to a {@link Server}. If used as {@link
 * java.io.Closeable} will unbind itself form its bound server.
 */
public class BoundCluster extends Cluster
    implements BoundTopic<ClusterConnectionReport, ClusterQueryLogReport> {

  private final transient Server server;

  private final transient StubStore stubStore;

  BoundCluster(Cluster delegate, Long clusterId, Server server) {
    super(
        delegate.getName(),
        clusterId,
        delegate.getCassandraVersion(),
        delegate.getDSEVersion(),
        delegate.getPeerInfo());
    this.server = server;
    this.stubStore = new StubStore();
  }

  /**
   * Convenience method to find the DataCenter with the given id.
   *
   * @param id id of the data center.
   * @return the data center if found or null.
   */
  public BoundDataCenter dc(long id) {
    return this.getDataCenters()
        .stream()
        .filter(dc -> dc.getId() == id)
        .findFirst()
        .map(dc -> (BoundDataCenter) dc)
        .orElse(null);
  }

  /**
   * Convenience method to find the Node with the given DataCenter id and node id.
   *
   * @param dcId id of the data center.
   * @param nodeId id of the node.
   * @return the node if found or null
   */
  public BoundNode node(long dcId, long nodeId) {
    BoundDataCenter dc = dc(dcId);
    if (dc != null) {
      return dc.node(nodeId);
    } else {
      return null;
    }
  }

  /**
   * Convenience method to find the Node in DataCenter 0 with the given id. This is a shortcut for
   * <code>node(0, X)</code> as it is common for clusters to only have 1 dc.
   *
   * @param nodeId id of the node.
   * @return the node if found in dc 0 or null
   */
  public BoundNode node(long nodeId) {
    return node(0, nodeId);
  }

  @Override
  public StubStore getStubStore() {
    return stubStore;
  }

  @Override
  public int clearPrimes(boolean nested) {
    int cleared = getStubStore().clear();
    if (nested) {
      for (BoundDataCenter dc : getBoundDataCenters()) {
        cleared += dc.clearPrimes(true);
      }
    }
    return cleared;
  }

  @Override
  public ClusterConnectionReport getConnections() {
    ClusterConnectionReport clusterConnectionReport = new ClusterConnectionReport(getId());
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
    return clusterConnectionReport;
  }

  @Override
  public CompletionStage<ClusterConnectionReport> closeConnectionsAsync(CloseType closeType) {
    ClusterConnectionReport report = getConnections();
    return CompletableFuture.allOf(
            this.getNodes()
                .stream()
                .map(n -> ((BoundNode) n).closeConnectionsAsync(closeType).toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> report);
  }

  @Override
  public CompletionStage<ClusterConnectionReport> closeConnectionAsync(
      SocketAddress connection, CloseType type) {

    for (Node node : this.getNodes()) {
      BoundNode boundNode = (BoundNode) node;
      // identify the node that has the connection and close it with that node.
      for (SocketAddress address : boundNode.getConnections().getConnections()) {
        if (connection.equals(address)) {
          return boundNode
              .closeConnectionAsync(address, type)
              .thenApply(NodeConnectionReport::getRootReport);
        }
      }
    }

    CompletableFuture<ClusterConnectionReport> failedFuture = new CompletableFuture<>();
    failedFuture.completeExceptionally(new IllegalArgumentException("Not found"));
    return failedFuture;
  }

  @JsonIgnore
  public List<BoundDataCenter> getBoundDataCenters() {
    return getDataCenters().stream().map(d -> (BoundDataCenter) d).collect(Collectors.toList());
  }

  @Override
  public List<BoundNode> getBoundNodes() {
    return getNodes().stream().map(n -> (BoundNode) n).collect(Collectors.toList());
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
    this.getBoundNodes().forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs()));
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
    this.getBoundNodes()
        .forEach(n -> clusterQueryLogReport.addNode(n, n.activityLog.getLogs(primed)));
    return clusterQueryLogReport;
  }

  @Override
  public BoundCluster getCluster() {
    return this;
  }

  @Override
  public void clearLogs() {
    getBoundNodes().forEach(BoundNode::clearLogs);
  }

  @Override
  public Server getServer() {
    return server;
  }

  Optional<StubMapping> find(Node node, Frame frame) {
    Optional<StubMapping> stub = stubStore.find(node, frame);
    if (!stub.isPresent() && server != null) {
      stub = server.stubStore.find(node, frame);
    }
    return stub;
  }
}
