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

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.stubbing.EmptyReturnMetadataHandler;
import com.datastax.oss.simulacron.common.stubbing.PeerMetadataHandler;
import com.datastax.oss.simulacron.server.token.RandomTokenAssigner;
import com.datastax.oss.simulacron.server.token.SplitTokenAssigner;
import com.datastax.oss.simulacron.server.token.TokenAssigner;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * The main point of entry for registering and binding Clusters to the network. Provides methods for
 * registering and unregistering Clusters. When a Cluster is registered, all applicable nodes are
 * bound to their respective network interfaces and are ready to handle native protocol traffic.
 */
public final class Server implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  static final AttributeKey<BoundNode> HANDLER = AttributeKey.valueOf("NODE");

  private static final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultServer(new ByteBufCodec(), Compressor.none());

  /** The bootstrap responsible for binding new listening server channels. */
  final ServerBootstrap serverBootstrap;

  /**
   * A resolver for deriving the next {@link SocketAddress} to assign a {@link NodeSpec} if it does
   * not provide one.
   */
  private final AddressResolver addressResolver;

  /**
   * The amount of time in nanoseconds to allow for waiting for network interfaces to bind for
   * nodes.
   */
  private final long bindTimeoutInNanos;

  /** The store that configures how to handle requests sent from a client. */
  final StubStore stubStore;

  /** The timer to use for scheduling actions. */
  final Timer timer;

  /** Whether or not a custom timer was used. We don't want to close ones users pass in. */
  private final boolean customTimer;

  /** Counter used to assign incrementing ids to clusters. */
  private final AtomicLong clusterCounter = new AtomicLong();

  /** Mapping of registered {@link BoundCluster} instances to their identifier. */
  private final Map<Long, BoundCluster> clusters = new ConcurrentHashMap<>();

  /** Whether or not activity logging is enabled. */
  private final boolean activityLogging;

  final EventLoopGroup eventLoopGroup;

  /** Whether or not a custom event loop was used. We don't want to close ones users pass in. */
  private final boolean customEventLoop;

  private final AtomicReference<CompletionStage<Void>> closeFuture = new AtomicReference<>();

  Server(
      AddressResolver addressResolver,
      EventLoopGroup eventLoopGroup,
      boolean customEventLoop,
      Timer timer,
      boolean customTimer,
      long bindTimeoutInNanos,
      StubStore stubStore,
      boolean activityLogging,
      ServerBootstrap serverBootstrap) {
    // custom constructor onyl made to help facilitate testing with a custom bootstrap.
    this.addressResolver = addressResolver;
    this.timer = timer;
    this.customTimer = customTimer;
    this.eventLoopGroup = eventLoopGroup;
    this.customEventLoop = customEventLoop;
    this.serverBootstrap = serverBootstrap;
    this.bindTimeoutInNanos = bindTimeoutInNanos;
    this.stubStore = stubStore;
    this.activityLogging = activityLogging;
  }

  private Server(
      AddressResolver addressResolver,
      EventLoopGroup eventLoopGroup,
      Class<? extends ServerChannel> channelClass,
      boolean customEventLoop,
      Timer timer,
      boolean customTimer,
      long bindTimeoutInNanos,
      StubStore stubStore,
      boolean activityLogging) {
    this(
        addressResolver,
        eventLoopGroup,
        customEventLoop,
        timer,
        customTimer,
        bindTimeoutInNanos,
        stubStore,
        activityLogging,
        new ServerBootstrap()
            .group(eventLoopGroup)
            .channel(channelClass)
            .childHandler(new Initializer()));
  }

  /** @return Whether or not this has been closed. */
  public boolean isClosed() {
    return closeFuture.get() != null;
  }

  <T> CompletionStage<T> failByClose() {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(new IllegalStateException("Server is closed"));
    return future;
  }

  /**
   * Wraps a Cluster in a {@link BoundCluster} which is a {@link java.io.Closeable} version of
   * Cluster
   *
   * @param cluster Cluster to wrap in a bound cluster.
   */
  @SuppressWarnings("unchecked")
  private BoundCluster boundCluster(ClusterSpec cluster) {
    long clusterId = cluster.getId() == null ? clusterCounter.getAndIncrement() : cluster.getId();
    return new BoundCluster(cluster, clusterId, this);
  }

  /** synchronous version of {@link #registerAsync(ClusterSpec.Builder)} */
  public BoundCluster register(ClusterSpec.Builder builder) {
    return getUninterruptibly(registerAsync(builder));
  }

  /** see {@link #registerAsync(ClusterSpec, ServerOptions)} */
  public CompletionStage<BoundCluster> registerAsync(ClusterSpec.Builder builder) {
    return registerAsync(builder.build(), ServerOptions.DEFAULT);
  }

  /** synchronous version of {@link #registerAsync(ClusterSpec)} */
  public BoundCluster register(ClusterSpec cluster) {
    return getUninterruptibly(registerAsync(cluster));
  }

  /** see {@link #registerAsync(ClusterSpec, ServerOptions)} */
  public CompletionStage<BoundCluster> registerAsync(ClusterSpec cluster) {
    return registerAsync(cluster, ServerOptions.DEFAULT);
  }

  /** synchronous version of {@link #registerAsync(ClusterSpec, ServerOptions)} */
  public BoundCluster register(ClusterSpec cluster, ServerOptions serverOptions) {
    return getUninterruptibly(registerAsync(cluster, serverOptions));
  }

  /**
   * Registers a {@link BoundCluster} and binds its {@link BoundNode} instances to their respective
   * interfaces. If any members of the cluster lack an id, this will assign a random one for them.
   * If any nodes lack an address, this will assign one based on the configured {@link
   * AddressResolver}.
   *
   * <p>The given future will fail if not completed after the configured {@link
   * Server.Builder#withBindTimeout(long, TimeUnit)}.
   *
   * @param cluster cluster to register
   * @param serverOptions custom server options to use when registering this cluster.
   * @return A future that when it completes provides a {@link BoundCluster} with ids and addresses
   *     assigned. Note that the return value is not the same object as the input.
   */
  @SuppressWarnings("unchecked")
  public CompletionStage<BoundCluster> registerAsync(
      ClusterSpec cluster, ServerOptions serverOptions) {
    if (isClosed()) {
      return failByClose();
    }
    BoundCluster c = boundCluster(cluster);

    List<CompletableFuture<BoundNode>> bindFutures = new ArrayList<>();

    boolean activityLogging =
        serverOptions.isActivityLoggingEnabled() != null
            ? serverOptions.isActivityLoggingEnabled()
            : this.activityLogging;

    TokenAssigner tokenAssignment =
        cluster.getNumberOfTokens() == 1
            ? new SplitTokenAssigner(cluster)
            : new RandomTokenAssigner(cluster.getNumberOfTokens());

    for (DataCenterSpec dataCenter : cluster.getDataCenters()) {
      BoundDataCenter dc = new BoundDataCenter(dataCenter, c);

      for (NodeSpec node : dataCenter.getNodes()) {
        String tokenStr = tokenAssignment.getTokens(node);
        // Use node's address if set, otherwise generate a new one.
        SocketAddress address =
            node.getAddress() != null ? node.getAddress() : addressResolver.get();
        bindFutures.add(
            bindInternal(node, c, dc, tokenStr, address, activityLogging).toCompletableFuture());
      }
    }

    clusters.put(c.getId(), c);

    List<BoundNode> nodes = new ArrayList<>(bindFutures.size());
    Throwable exception = null;
    boolean timedOut = false;
    // Evaluate each future, if any fail capture the exception and record all successfully
    // bound nodes.
    long start = System.nanoTime();
    for (CompletableFuture<BoundNode> f : bindFutures) {
      try {
        if (timedOut) {
          BoundNode node = f.getNow(null);
          if (node != null) {
            nodes.add(node);
          }
        } else {
          nodes.add(f.get(bindTimeoutInNanos, TimeUnit.NANOSECONDS));
        }
        if (System.nanoTime() - start > bindTimeoutInNanos) {
          timedOut = true;
        }
      } catch (TimeoutException te) {
        timedOut = true;
        exception = te;
      } catch (Exception e) {
        exception = e.getCause();
      }
    }

    // If there was a failure close all bound nodes.
    if (exception != null) {
      final Throwable e = exception;
      // Close each node
      List<CompletableFuture<BoundNode>> futures =
          nodes.stream().map(this::close).collect(Collectors.toList());

      CompletableFuture<BoundCluster> future = new CompletableFuture<>();
      // On completion of unbinding all nodes, fail the returned future.
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
          .handle(
              (v, ex) -> {
                // remove cluster from registry since it failed to completely register.
                clusters.remove(c.getId());
                future.completeExceptionally(e);
                return v;
              });
      return future;
    } else {
      return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
          .thenApply(__ -> c);
    }
  }

  /** synchronous version of {@link #unregisterAsync(BoundNode)} */
  public BoundCluster unregister(BoundNode node) {
    return getUninterruptibly(unregisterAsync(node));
  }

  /**
   * Convenience method for unregistering NodeSpec's cluster by object instead of by id as in {@link
   * #unregisterAsync(Long)}.
   *
   * @param node NodeSpec to unregister
   * @return A future that when completed provides the unregistered cluster as it existed in the
   *     registry, may not be the same object as the input.
   */
  public CompletionStage<BoundCluster> unregisterAsync(BoundNode node) {
    if (isClosed()) {
      return failByClose();
    }
    if (node.getCluster() == null) {
      CompletableFuture<BoundCluster> future = new CompletableFuture<>();
      future.completeExceptionally(new IllegalArgumentException("Node has no parent Cluster"));
      return future;
    }
    return unregisterAsync(node.getCluster().getId());
  }

  /** synchronous version of {@link #unregisterAsync(BoundCluster)} */
  public BoundCluster unregister(BoundCluster cluster) {
    return getUninterruptibly(unregisterAsync(cluster));
  }

  /**
   * Convenience method for unregistering Cluster by object instead of by id as in {@link
   * #unregisterAsync(Long)}.
   *
   * @param cluster Cluster to unregister
   * @return A future that when completed provides the unregistered cluster as it existed in the
   *     registry, may not be the same object as the input.
   */
  public CompletionStage<BoundCluster> unregisterAsync(BoundCluster cluster) {
    return unregisterAsync(cluster.getId());
  }

  /** synchronous version of {@link #unregisterAsync(Long)} */
  public BoundCluster unregister(Long clusterId) {
    return getUninterruptibly(unregisterAsync(clusterId));
  }

  /**
   * Unregisters a cluster and closes all listening network interfaces associated with it.
   *
   * <p>If the cluster is not currently registered the returned future will fail with an {@link
   * IllegalArgumentException}.
   *
   * @param clusterId id of the cluster.
   * @return A future that when completed provides the unregistered cluster as it existed in the
   *     registry, may not be the same object as the input.
   */
  public CompletionStage<BoundCluster> unregisterAsync(Long clusterId) {
    if (isClosed()) {
      return failByClose();
    }
    CompletableFuture<BoundCluster> future = new CompletableFuture<>();
    if (clusterId == null) {
      future.completeExceptionally(new IllegalArgumentException("Null id provided"));
    } else {
      BoundCluster foundCluster = clusters.remove(clusterId);
      List<CompletableFuture<BoundNode>> closeFutures = new ArrayList<>();
      if (foundCluster != null) {
        // Close socket on each node.
        for (BoundDataCenter dataCenter : foundCluster.getDataCenters()) {
          for (BoundNode node : dataCenter.getNodes()) {
            closeFutures.add(close(node));
          }
        }
        CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[] {}))
            .whenComplete(
                (__, ex) -> {
                  if (ex != null) {
                    future.completeExceptionally(ex);
                  } else {
                    future.complete(foundCluster);
                  }
                });
      } else {
        future.completeExceptionally(new IllegalArgumentException("ClusterSpec not found."));
      }
    }

    return future;
  }

  /** synchronous version of {@link #unregisterAllAsync()} */
  public Integer unregisterAll() {
    return getUninterruptibly(unregisterAllAsync());
  }

  /**
   * Unregister all currently registered clusters.
   *
   * @return future that is completed when all clusters are unregistered.
   */
  public CompletionStage<Integer> unregisterAllAsync() {
    if (isClosed()) {
      return failByClose();
    }
    List<CompletableFuture<BoundCluster>> futures =
        clusters
            .keySet()
            .stream()
            .map(this::unregisterAsync)
            .map(CompletionStage::toCompletableFuture)
            .collect(Collectors.toList());

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
        .thenApply(__ -> futures.size());
  }

  /** synchronous version of {@link #registerAsync(NodeSpec.Builder)} */
  public BoundNode register(NodeSpec.Builder builder) {
    return getUninterruptibly(registerAsync(builder));
  }

  /** see {@link #registerAsync(NodeSpec, ServerOptions)} */
  public CompletionStage<BoundNode> registerAsync(NodeSpec.Builder builder) {
    return registerAsync(builder.build());
  }

  /** see {@link #registerAsync(NodeSpec, ServerOptions)} */
  public BoundNode register(NodeSpec node) {
    return getUninterruptibly(registerAsync(node));
  }

  /** see {@link #registerAsync(NodeSpec, ServerOptions)} */
  public CompletionStage<BoundNode> registerAsync(NodeSpec node) {
    return registerAsync(node, ServerOptions.DEFAULT);
  }

  /** synchronous version of {@link #registerAsync(NodeSpec, ServerOptions)} */
  public BoundNode register(NodeSpec node, ServerOptions serverOptions) {
    return getUninterruptibly(registerAsync(node, serverOptions));
  }

  /**
   * Convenience method that registers a {@link NodeSpec} by wrapping it in a 'dummy' {@link
   * ClusterSpec} and registering that.
   *
   * <p>Note that if the given {@link NodeSpec} belongs to a {@link ClusterSpec}, the returned
   * future will fail with an {@link IllegalArgumentException}.
   *
   * @param node node to register.
   * @param serverOptions custom server options to use when registering this node.
   * @return A future that when it completes provides an updated {@link ClusterSpec} with ids and
   *     addresses assigned. Note that the return value is not the same object as the input.
   */
  public CompletionStage<BoundNode> registerAsync(NodeSpec node, ServerOptions serverOptions) {
    if (isClosed()) {
      return failByClose();
    }
    if (node.getDataCenter() != null) {
      CompletableFuture<BoundNode> future = new CompletableFuture<>();
      future.completeExceptionally(
          new IllegalArgumentException("Node belongs to a Cluster, should be standalone."));
      return future;
    }
    // Wrap node in dummy cluster
    Long clusterId = clusterCounter.getAndIncrement();
    BoundCluster dummyCluster =
        boundCluster(ClusterSpec.builder().withId(clusterId).withName("dummy").build());
    BoundDataCenter dummyDataCenter = new BoundDataCenter(dummyCluster);

    clusters.put(clusterId, dummyCluster);

    boolean activityLogging =
        serverOptions.isActivityLoggingEnabled() != null
            ? serverOptions.isActivityLoggingEnabled()
            : this.activityLogging;
    // Use node's address if set, otherwise generate a new one.
    SocketAddress address = node.getAddress() != null ? node.getAddress() : addressResolver.get();
    return bindInternal(
        node,
        dummyCluster,
        dummyDataCenter,
        node.resolvePeerInfo("tokens", String.class).orElse("0"),
        address,
        activityLogging);
  }

  /**
   * @param id identifier of the cluster
   * @return cluster matching given id or null.
   */
  public BoundCluster getCluster(long id) {
    return this.clusters.get(id);
  }

  /** @return All clusters currently registered to this server. */
  public Collection<BoundCluster> getClusters() {
    return this.clusters.values();
  }

  private CompletionStage<BoundNode> bindInternal(
      NodeSpec refNode,
      BoundCluster cluster,
      BoundDataCenter parent,
      String token,
      SocketAddress address,
      boolean activityLogging) {
    // derive a token for this node. This is done here as the ordering of nodes under a
    // data center is changed when it is bound.
    Map<String, Object> newPeerInfo = new HashMap<>(refNode.getPeerInfo());
    newPeerInfo.put("tokens", token);
    CompletableFuture<BoundNode> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(address);
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              if (channelFuture.isSuccess()) {
                BoundNode node =
                    new BoundNode(
                        address,
                        refNode.getName(),
                        refNode.getId() != null
                            ? refNode.getId()
                            : 0, // assign id 0 if this is a standalone node.
                        refNode.getCassandraVersion(),
                        refNode.getDSEVersion(),
                        newPeerInfo,
                        cluster,
                        parent,
                        this,
                        timer,
                        channelFuture.channel(),
                        activityLogging);
                logger.info("Bound Node {} to {}", node.resolveId(), channelFuture.channel());
                channelFuture.channel().attr(HANDLER).set(node);
                f.complete(node);
              } else {
                // If failed, propagate it.
                f.completeExceptionally(
                    new BindNodeException(refNode, address, channelFuture.cause()));
              }
            });

    return f;
  }

  private CompletableFuture<BoundNode> close(BoundNode node) {
    logger.debug("Closing Node {} on {}.", node.resolveId(), node.channel);
    return node.stopAsync()
        .thenApply(
            n -> {
              logger.debug(
                  "Releasing {} back to address resolver so it may be reused.", node.getAddress());
              addressResolver.release(node.getAddress());
              return node;
            })
        .toCompletableFuture();
  }

  /**
   * Wrapper method for using netty-transport-native-epoll if available, addresses classes directly
   * without importing so if library isn't in environment this doesn't create any problems.
   *
   * @return Epoll-based event loop group is epoll is available, otherwise empty.
   */
  private static Optional<EventLoopGroup> epollEventLoopGroup(ThreadFactory threadFactory) {
    if (io.netty.channel.epoll.Epoll.isAvailable()) {
      return Optional.of(new io.netty.channel.epoll.EpollEventLoopGroup(0, threadFactory));
    }
    return Optional.empty();
  }

  private static Class<? extends ServerChannel> epollClass() {
    return io.netty.channel.epoll.EpollServerSocketChannel.class;
  }

  /** @return a {@link Builder} for configuring and creating {@link Server} instances. */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Closes Server and all of its resources. First unregisters all Clusters then closes event loop
   * and timer unless they were provided as input into {@link Server.Builder}.
   *
   * @return future that completes when Server is freed.
   */
  public CompletionStage<Void> closeAsync() {
    if (isClosed()) {
      return closeFuture.get();
    } else {
      return closeFuture.updateAndGet(
          current -> {
            if (current != null) {
              return current;
            } else {
              return this.unregisterAllAsync()
                  .thenCompose(
                      i -> {
                        // If timer was created for Server, stop it.
                        if (!customTimer) {
                          timer.stop();
                        }
                        // If event loop was created for Server, shut it down.
                        if (!customEventLoop) {
                          // Immediate shutdown should be appropriate since we unregister first so nothing should be
                          // happening on event loop.
                          Future<?> future =
                              eventLoopGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
                          // adapt future to completable future by calling get on common pool.
                          CompletableFuture<Void> f =
                              CompletableFuture.supplyAsync(
                                  () -> {
                                    try {
                                      future.get();
                                      return null;
                                    } catch (InterruptedException | ExecutionException e) {
                                      throw new RuntimeException(e);
                                    }
                                  });
                          return f;
                        } else {
                          CompletableFuture<Void> future = new CompletableFuture<>();
                          future.complete(null);
                          return future;
                        }
                      });
            }
          });
    }
  }

  /**
   * @inheritDoc
   *     <p>Also see {@link #closeAsync()}
   */
  @Override
  public void close() {
    getUninterruptibly(closeAsync());
  }

  public static class Builder {
    private AddressResolver addressResolver = AddressResolver.defaultResolver;

    private static long DEFAULT_BIND_TIMEOUT_IN_NANOS =
        TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    private long bindTimeoutInNanos = DEFAULT_BIND_TIMEOUT_IN_NANOS;

    private Timer timer;

    private StubStore stubStore;

    private boolean activityLogging = true;

    private EventLoopGroup eventLoopGroup;

    private Class<? extends ServerChannel> channelClass;

    Builder() {}

    /**
     * Sets the bind timeout which is the amount of time allowed while binding listening interfaces
     * for nodes when establishing a cluster.
     *
     * @param time The amount of time to wait.
     * @param timeUnit The unit of time to wait in.
     * @return This builder.
     */
    public Builder withBindTimeout(long time, TimeUnit timeUnit) {
      this.bindTimeoutInNanos = TimeUnit.NANOSECONDS.convert(time, timeUnit);
      return this;
    }

    /**
     * Sets the address resolver to use when assigning {@link SocketAddress} to {@link NodeSpec}'s
     * that don't have previously provided addresses.
     *
     * @param addressResolver resolver to use.
     * @return This builder.
     */
    public Builder withAddressResolver(AddressResolver addressResolver) {
      this.addressResolver = addressResolver;
      return this;
    }

    /**
     * Sets a pre-created event loop group to use for the created servers {@link ServerBootstrap}.
     * This will not be closed when the server is closed.
     *
     * <p>If not specified, an Epoll or NIO event loop will be created with Server that is closed
     * when Server closes. Threads created by this event loop will be named 'simulacron-worker-X-Y'.
     *
     * @param eventLoopGroup event loop to use for Server
     * @param clazz The expected channel class to be created with this event loop.
     * @return This builder.
     */
    public Builder withEventLoopGroup(
        EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> clazz) {
      this.eventLoopGroup = eventLoopGroup;
      this.channelClass = clazz;
      return this;
    }

    /**
     * Sets the timer to use for scheduling actions. If not set, a {@link HashedWheelTimer} is
     * created with a naming format of 'simulacron-timer-X-Y'.
     *
     * @param timer timer to use.
     * @return This builder.
     */
    public Builder withTimer(Timer timer) {
      this.timer = timer;
      return this;
    }

    /**
     * Sets the {@link StubStore} to be used by this server. By default creates a new one with
     * built-in stubs for handling metadata requests for system.local and peers ({@link
     * PeerMetadataHandler})
     *
     * @param stubStore stub store to use.
     * @return This builder.
     */
    public Builder withStubStore(StubStore stubStore) {
      this.stubStore = stubStore;
      return this;
    }

    /**
     * Whether or not to enable activity logging. By default it is enabled.
     *
     * @param enabled enablement flag.
     * @return This builder.
     */
    public Builder withActivityLoggingEnabled(boolean enabled) {
      this.activityLogging = enabled;
      return this;
    }

    /** @return a {@link Server} instance based on this builder's configuration. */
    public Server build() {
      if (stubStore == null) {
        stubStore = new StubStore();
        stubStore.register(new PeerMetadataHandler());

        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.keyspaces"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.views"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.tables"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.columns"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.indexes"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.triggers"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.types"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.functions"));
        stubStore.register(
            new EmptyReturnMetadataHandler("SELECT * FROM system_schema.aggregates"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system_schema.views"));

        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system.schema_keyspaces"));
        stubStore.register(
            new EmptyReturnMetadataHandler("SELECT * FROM system.schema_columnfamilies"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system.schema_columns"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system.schema_triggers"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system.schema_usertypes"));
        stubStore.register(new EmptyReturnMetadataHandler("SELECT * FROM system.schema_functions"));
        stubStore.register(
            new EmptyReturnMetadataHandler("SELECT * FROM system.schema_aggregates"));
      }
      Timer timer = this.timer;
      if (timer == null) {
        ThreadFactory f = new DefaultThreadFactory("simulacron-timer");
        timer = new HashedWheelTimer(f);
      }

      EventLoopGroup eventLoopGroup = this.eventLoopGroup;
      Class<? extends ServerChannel> channelClass = this.channelClass;
      if (eventLoopGroup == null) {
        ThreadFactory f = new DefaultThreadFactory("simulacron-io-worker");
        try {
          // try to resolve Epoll class, if throws Exception, fall back on nio
          Class.forName("io.netty.channel.epoll.Epoll");
          // if epoll event loop could be established, use it, otherwise use nio.
          Optional<EventLoopGroup> epollEventLoop = epollEventLoopGroup(f);
          if (epollEventLoop.isPresent()) {
            logger.debug("Detected epoll support, using EpollEventLoopGroup");
            eventLoopGroup = epollEventLoop.get();
            channelClass = epollClass();
          } else {
            logger.debug("Could not load native transport (epoll), using NioEventLoopGroup");
            eventLoopGroup = new NioEventLoopGroup(0, f);
            channelClass = NioServerSocketChannel.class;
          }
        } catch (ClassNotFoundException ce) {
          logger.debug("netty-transport-native-epoll not on classpath, using NioEventLoopGroup");
          eventLoopGroup = new NioEventLoopGroup(0, f);
          channelClass = NioServerSocketChannel.class;
        }
      }
      return new Server(
          addressResolver,
          eventLoopGroup,
          channelClass,
          this.eventLoopGroup != null,
          timer,
          this.timer != null,
          bindTimeoutInNanos,
          stubStore,
          activityLogging);
    }
  }

  static class Initializer extends ChannelInitializer<Channel> {

    @Override
    protected void initChannel(Channel channel) throws Exception {
      ChannelPipeline pipeline = channel.pipeline();
      BoundNode node = channel.parent().attr(HANDLER).get();
      node.clientChannelGroup.add(channel);
      MDC.put("node", node.getId().toString());

      try {
        logger.debug("Got new connection {}", channel);

        pipeline
            .addLast("decoder", new FrameDecoder(frameCodec))
            .addLast("encoder", new FrameEncoder(frameCodec))
            .addLast("requestHandler", new RequestHandler(node));
      } finally {
        MDC.remove("node");
      }
    }
  }
}
