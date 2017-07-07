package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.ConnectionReport;
import com.datastax.simulacron.common.cluster.NodeProperties;
import com.datastax.simulacron.common.cluster.QueryLog;
import com.datastax.simulacron.common.stubbing.CloseType;
import com.datastax.simulacron.common.stubbing.Prime;
import com.datastax.simulacron.common.stubbing.PrimeDsl;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.SocketAddress;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.datastax.simulacron.server.CompletableFutures.getUninterruptibly;

public interface BoundTopic<C extends ConnectionReport> extends AutoCloseable, NodeProperties {

  StubStore getStubStore();

  /**
   * Primes the given query. All nodes associated with this will use this prime.
   *
   * @param prime prime to register
   */
  default void prime(Prime prime) {
    getStubStore().register(prime);
  }

  /**
   * Convenience for {@link #prime(Prime)} which builds the input builder and passes it as input.
   *
   * @param prime prime to register
   */
  default void prime(PrimeDsl.PrimeBuilder prime) {
    prime(prime.build());
  }

  /** synchronous version of {@link #unregister()} */
  default BoundCluster unregister() {
    return getUninterruptibly(unregisterAsync());
  }

  /**
   * Unregisters the associated Cluster from the Server.
   *
   * @return unregistered Cluster
   */
  default CompletionStage<BoundCluster> unregisterAsync() {
    return getServer().unregisterAsync(getBoundCluster());
  }

  @JsonIgnore
  C getConnections();

  /** synchronous version of {@link #closeConnectionsAsync(CloseType)} */
  default C closeConnections(CloseType closeType) {
    return getUninterruptibly(closeConnectionsAsync(closeType));
  }

  /**
   * Closes all connections
   *
   * @param closeType way of closing connection
   * @return the closed connections
   */
  CompletionStage<C> closeConnectionsAsync(CloseType closeType);

  /** synchronous version of {@link #closeConnectionAsync(SocketAddress, CloseType)} */
  default C closeConnection(SocketAddress connection, CloseType closeType) {
    return getUninterruptibly(closeConnectionAsync(connection, closeType));
  }

  /**
   * Closes a particular connection
   *
   * @param connection connection to close
   * @param type way of closing the connection
   * @return report for the closed connection
   */
  CompletionStage<C> closeConnectionAsync(SocketAddress connection, CloseType type);

  /** @return All nodes belonging to this topic. */
  @JsonIgnore
  Stream<BoundNode> getBoundNodes();

  /**
   * Apply a function that returns a CompletableFuture on each node.
   *
   * @param fun Function to apply
   * @return future result of applying function on each node.
   */
  default CompletionStage<Void> forEachNode(Function<BoundNode, CompletionStage<Void>> fun) {
    return CompletableFuture.allOf(
            this.getBoundNodes()
                .map(i -> fun.apply(i).toCompletableFuture())
                .collect(Collectors.toList())
                .toArray(new CompletableFuture[] {}))
        .thenApply(v -> null);
  }

  /** synchronous version of {@link #stopAsync()} */
  default void stop() {
    getUninterruptibly(stopAsync());
  }

  /**
   * Convenience method for 'stopping' nodes. Shortcut for rejectConnectionsAsync with STOP reject
   * scope.
   *
   * @return future that completes when stopAsync completes.
   */
  default CompletionStage<Void> stopAsync() {
    return rejectConnectionsAsync(0, RejectScope.STOP);
  }

  /** synchronous version of {@link #rejectConnectionsAsync(int, RejectScope)} */
  default void rejectConnections(int after, RejectScope rejectScope) {
    getUninterruptibly(rejectConnectionsAsync(after, rejectScope));
  }

  /**
   * Rejects establishing new connections in the configured way.
   *
   * @param after number of connection attempts after which the rejection will be applied
   * @param rejectScope type of the rejection
   * @return future that completes when reject completes
   */
  default CompletionStage<Void> rejectConnectionsAsync(int after, RejectScope rejectScope) {
    return forEachNode(n -> n.rejectConnectionsAsync(after, rejectScope));
  }

  /** synchronous version of {@link #acceptConnectionsAsync()} */
  default void acceptConnections() {
    getUninterruptibly(acceptConnectionsAsync());
  }

  /**
   * Method to configure a cluster, dc, or node so they accept future connections. Usually called
   * after {@link #rejectConnectionsAsync(int, RejectScope)}.
   *
   * @return future that completes when accept completes
   */
  default CompletionStage<Void> acceptConnectionsAsync() {
    return forEachNode(BoundNode::acceptConnectionsAsync);
  }

  /** synchronous version of {@link #startAsync()} */
  default void start() {
    getUninterruptibly(startAsync());
  }

  /**
   * Convenience method for 'starting' a previous stopped/rejected cluster, dc, or node. Shortcut
   * for {@link #acceptConnectionsAsync}.
   *
   * @return future that completes when start completes
   */
  default CompletionStage<Void> startAsync() {
    return acceptConnectionsAsync();
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
  default void close() {
    try {
      unregister();
    } catch (IllegalArgumentException ex) {
      // cluster was no longer bound to server, this is ok.
    }
  }
}
