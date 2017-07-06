package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.ConnectionReport;
import com.datastax.simulacron.common.cluster.NodeProperties;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.common.stubbing.CloseType;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.PrimeDsl;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface BoundTopic<C extends ConnectionReport> extends Closeable, NodeProperties {

  StubStore getStubStore();

  default void prime(Prime prime) {
    getStubStore().register(prime);
  }

  default void prime(PrimeDsl.PrimeBuilder prime) {
    prime(prime.build());
  }

  default CompletableFuture<BoundCluster> unregister() {
    return getServer().unregister(getBoundCluster());
  }

  @JsonIgnore
  C getConnections();

  /**
   * Closes all connections
   *
   * @param closeType way of closing connection
   * @return the closed connections
   */
  CompletableFuture<C> closeConnections(CloseType closeType);

  /**
   * Closes a particular connection
   *
   * @param connection connection to close
   * @param type way of closing the connection
   * @return report for the closed connection
   */
  CompletableFuture<C> closeConnection(SocketAddress connection, CloseType type);

  /** @return All nodes belonging to this topic. */
  @JsonIgnore
  Stream<BoundNode> getBoundNodes();

  /**
   * Apply a function that returns a CompletableFuture on each node.
   *
   * @param fun Function to apply
   * @return future result of applying function on each node.
   */
  default CompletableFuture<Void> forEachNode(Function<BoundNode, CompletableFuture<Void>> fun) {
    return CompletableFuture.allOf(
            this.getBoundNodes()
                .map(fun)
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> null);
  }

  /**
   * Convenience method for 'stopping' nodes. Shortcut for rejectConnections with STOP reject scope.
   *
   * @return future that completes when stop completes.
   */
  default CompletableFuture<Void> stop() {
    return rejectConnections(0, RejectScope.STOP);
  }

  /**
   * Rejects establishing new connections in the configured way.
   *
   * @param after number of connection attempts after which the rejection will be applied
   * @param rejectScope type of the rejection
   * @return future that completes when reject completes
   */
  default CompletableFuture<Void> rejectConnections(int after, RejectScope rejectScope) {
    return forEachNode(n -> n.rejectConnections(after, rejectScope));
  }

  /**
   * Method to configure a cluster, dc, or node so they accept future connections. Usually called
   * after {@link #rejectConnections(int, RejectScope)}.
   *
   * @return future that completes when accept completes
   */
  default CompletableFuture<Void> acceptConnections() {
    return forEachNode(BoundNode::acceptConnections);
  }

  /**
   * Convenience method for 'starting' a previous stopped/rejected cluster, dc, or node. Shortcut
   * for {@link #acceptConnections}.
   *
   * @return future that completes when start completes
   */
  default CompletableFuture<Void> start() {
    return acceptConnections();
  }

  /** @return recorded query logs for this. */
  @JsonIgnore
  List<QueryLog> getLogs();

  /** clears the query logs for this. */
  void clearLogs();

  @JsonIgnore
  BoundCluster getBoundCluster();

  @JsonIgnore
  Server getServer();

  @Override
  default void close() throws IOException {
    try {
      getServer().unregister(getBoundCluster()).join();
    } catch (CompletionException ce) {
      // if already unregistered no op.
      if (!(ce.getCause() instanceof IllegalArgumentException)) {
        throw ce;
      }
    }
  }
}
