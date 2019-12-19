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

import static com.datastax.oss.simulacron.server.CompletableFutures.getUninterruptibly;

import com.datastax.oss.simulacron.common.cluster.ConnectionReport;
import com.datastax.oss.simulacron.common.cluster.NodeProperties;
import com.datastax.oss.simulacron.common.cluster.QueryLog;
import com.datastax.oss.simulacron.common.cluster.QueryLogReport;
import com.datastax.oss.simulacron.common.stubbing.CloseType;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.stubbing.PrimeDsl;
import com.datastax.oss.simulacron.server.listener.QueryListener;
import com.fasterxml.jackson.annotation.JsonIgnore;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public interface BoundTopic<C extends ConnectionReport, Q extends QueryLogReport>
    extends AutoCloseable, NodeProperties {

  @JsonIgnore
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

  /**
   * Clears all primes associated with this.
   *
   * @param nested Also clears primes for underlying members (i.e. for a DC, also clears it's node's
   *     primes.)
   */
  int clearPrimes(boolean nested);

  /** synchronous version of {@link #unregisterAsync()} */
  default BoundCluster unregister() {
    return getUninterruptibly(unregisterAsync());
  }

  /**
   * Unregisters the associated Cluster from the Server.
   *
   * @return unregistered Cluster
   */
  CompletionStage<BoundCluster> unregisterAsync();

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

  /**
   * Pauses reading on all connections.
   *
   * @return report for the connections
   */
  C pauseRead();

  /**
   * Resume reading on all connections.
   *
   * @return report for the connections
   */
  C resumeRead();

  /** @return All nodes belonging to this topic. */
  @JsonIgnore
  Collection<BoundNode> getNodes();

  /**
   * Apply a function that returns a CompletableFuture on each node.
   *
   * @param fun Function to apply
   * @return future result of applying function on each node.
   */
  default CompletionStage<Void> forEachNode(Function<BoundNode, CompletionStage<Void>> fun) {
    return CompletableFuture.allOf(
            this.getNodes()
                .stream()
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
  Q getLogs();

  /**
   * @param primed whether or not query had a matching prime.
   * @return recorded query logs for this filtered by whether or not query had a matching prime.
   */
  @JsonIgnore
  Q getLogs(boolean primed);

  /** clears the query logs for this. */
  default void clearLogs() {
    getNodes().forEach(BoundNode::clearLogs);
  }

  /**
   * Registers a listener that is invoked whenever a query is received.
   *
   * @param queryListener The listener to invoke
   */
  default void registerQueryListener(QueryListener queryListener) {
    registerQueryListener(queryListener, false);
  }

  /**
   * Registers a listener that is invoked whenever a query is received.
   *
   * @param queryListener The listener to invoke
   * @param after Whether or not to invoke before the query is handled or after the actions for the
   *     query are all handled.
   */
  default void registerQueryListener(QueryListener queryListener, boolean after) {
    registerQueryListener(queryListener, after, BoundNode.ALWAYS_TRUE);
  }

  /**
   * Registers a listener that is invoked whenever a query is received and the filter predicate is
   * matched.
   *
   * @param queryListener The listener to invoke
   * @param after Whether or not to invoke before the query is handled (false) or after the actions
   *     for the query are all handled (true).
   * @param filter predicate to indicate whether or not to invoke the listener
   */
  void registerQueryListener(
      QueryListener queryListener, boolean after, Predicate<QueryLog> filter);

  @JsonIgnore
  Server getServer();

  @JsonIgnore
  FrameCodecWrapper getFrameCodec();

  @Override
  default void close() {
    try {
      unregister();
    } catch (IllegalArgumentException ex) {
      // cluster was no longer bound to server, this is ok.
    }
  }
}
