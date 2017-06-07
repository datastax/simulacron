package com.datastax.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.simulacron.common.cluster.AbstractNodeProperties;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.stubbing.EmptyReturnMetadataHandler;
import com.datastax.simulacron.common.stubbing.PeerMetadataHandler;
import com.datastax.simulacron.common.stubbing.StubMapping;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.AttributeKey;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * The main point of entry for registering and binding Clusters to the network. Provides methods for
 * registering and unregistering Clusters. When a Cluster is registered, all applicable nodes are
 * bound to their respective network interfaces and are ready to handle native protocol traffic.
 */
public final class Server {

  private static final Logger logger = LoggerFactory.getLogger(Server.class);

  static final AttributeKey<BoundNode> HANDLER = AttributeKey.valueOf("NODE");

  private static final FrameCodec<ByteBuf> frameCodec =
      FrameCodec.defaultServer(new ByteBufCodec(), Compressor.none());

  /** The bootstrap responsible for binding new listening server channels. */
  private final ServerBootstrap serverBootstrap;

  /**
   * A resolver for deriving the next {@link SocketAddress} to assign a {@link Node} if it does not
   * provide one.
   */
  private final AddressResolver addressResolver;

  /**
   * The amount of time in nanoseconds to allow for waiting for network interfaces to bind for
   * nodes.
   */
  private final long bindTimeoutInNanos;

  /** The store that configures how to handle requests sent from a client. */
  private final StubStore stubStore;

  /** The timer to use for scheduling actions. */
  private final Timer timer;

  /** Counter used to assign incrementing ids to clusters. */
  private final AtomicLong clusterCounter = new AtomicLong();

  /** Mapping of registered {@link Cluster} instances to their identifier. */
  private final Map<Long, Cluster> clusters = new ConcurrentHashMap<>();

  /** Whether or not activity logging is enabled. */
  private final boolean activityLogging;

  private Server(
      AddressResolver addressResolver,
      ServerBootstrap serverBootstrap,
      Timer timer,
      long bindTimeoutInNanos,
      StubStore stubStore,
      boolean activityLogging) {
    this.serverBootstrap = serverBootstrap;
    this.addressResolver = addressResolver;
    this.timer = timer;
    this.bindTimeoutInNanos = bindTimeoutInNanos;
    this.stubStore = stubStore;
    this.activityLogging = activityLogging;
  }

  /**
   * Provide a means to add a new StubMapping that can be used by the native server to provide
   * responses to various client requets.
   *
   * @param stubMapping to register
   */
  public void registerStub(StubMapping stubMapping) {
    this.stubStore.register(stubMapping);
  }

  /**
   * Clears all stubmappings that match a specific classtype
   *
   * @param clazz all stubmapping matching this class type will be removed
   * @return number of primes deleted
   */
  public int clearStubsMatchingType(Class clazz) {
    return this.stubStore.clearAllMatchingType(clazz);
  }

  /** see {@link #register(Cluster, ServerOptions)} */
  public CompletableFuture<Cluster> register(Cluster cluster) {
    return register(cluster, ServerOptions.DEFAULT);
  }

  /**
   * Registers a {@link Cluster} and binds it's {@link Node} instances to their respective
   * interfaces. If any members of the cluster lack an id, this will assign a random one for them.
   * If any nodes lack an address, this will assign one based on the configured {@link
   * AddressResolver}.
   *
   * <p>The given future will fail if not completed after the configured {@link
   * Server.Builder#withBindTimeout(long, TimeUnit)}.
   *
   * @param cluster cluster to register
   * @param serverOptions custom server options to use when registering this cluster.
   * @return A future that when it completes provides an updated {@link Cluster} with ids and
   *     addresses assigned. Note that the return value is not the same object as the input.
   */
  public CompletableFuture<Cluster> register(Cluster cluster, ServerOptions serverOptions) {
    long clusterId = clusterCounter.getAndIncrement();
    Cluster c = Cluster.builder().copy(cluster).withId(clusterId).build();

    List<CompletableFuture<Node>> bindFutures = new ArrayList<>();

    boolean activityLogging =
        serverOptions.isActivityLoggingEnabled() != null
            ? serverOptions.isActivityLoggingEnabled()
            : this.activityLogging;
    int dcPos = 0;
    int nPos = 0;
    for (DataCenter dataCenter : cluster.getDataCenters()) {
      long dcOffset = dcPos * 100;
      long nodeBase = ((long) Math.pow(2, 64) / dataCenter.getNodes().size());
      DataCenter dc = c.addDataCenter().copy(dataCenter).build();

      for (Node node : dataCenter.getNodes()) {
        Optional<Object> token = node.resolvePeerInfo("token");
        String tokenStr;
        if (token.isPresent()) {
          tokenStr = token.get().toString();
        } else if (node.getCluster() == null) {
          tokenStr = "0";
        } else {
          long nodeOffset = nPos * nodeBase;
          tokenStr = "" + (nodeOffset + dcOffset);
        }
        bindFutures.add(bindInternal(node, dc, tokenStr, activityLogging));
        nPos++;
      }
      dcPos++;
    }

    clusters.put(clusterId, c);

    List<BoundNode> nodes = new ArrayList<>(bindFutures.size());
    Throwable exception = null;
    boolean timedOut = false;
    // Evaluate each future, if any fail capture the exception and record all successfully
    // bound nodes.
    long start = System.nanoTime();
    for (CompletableFuture<Node> f : bindFutures) {
      try {
        if (timedOut) {
          Node node = f.getNow(null);
          if (node != null) {
            nodes.add((BoundNode) node);
          }
        } else {
          nodes.add((BoundNode) f.get(bindTimeoutInNanos, TimeUnit.NANOSECONDS));
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
      List<CompletableFuture<Node>> futures =
          nodes.stream().map(this::close).collect(Collectors.toList());

      CompletableFuture<Cluster> future = new CompletableFuture<>();
      // On completion of unbinding all nodes, fail the returned future.
      CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
          .handle(
              (v, ex) -> {
                // remove cluster from registry since it failed to completely register.
                clusters.remove(clusterId);
                future.completeExceptionally(e);
                return v;
              });
      return future;
    } else {
      return CompletableFuture.allOf(bindFutures.toArray(new CompletableFuture[] {}))
          .thenApply(__ -> c);
    }
  }

  /**
   * Unregisters a {@link Cluster} and closes all listening network interfaces associated with it.
   *
   * <p>If the cluster is not currently registered the returned future will fail with an {@link
   * IllegalArgumentException}.
   *
   * @param clusterId id of the cluster.
   * @return A future that when completed provides the unregistered cluster as it existed in the
   *     registry, may not be the same object as the input.
   */
  public CompletableFuture<Cluster> unregister(Long clusterId) {
    CompletableFuture<Cluster> future = new CompletableFuture<>();
    if (clusterId == null) {
      future.completeExceptionally(new IllegalArgumentException("Null id provided"));
    } else {
      Cluster foundCluster = clusters.get(clusterId);
      List<CompletableFuture<Node>> closeFutures = new ArrayList<>();
      if (foundCluster != null) {
        // Close socket on each node.
        for (DataCenter dataCenter : foundCluster.getDataCenters()) {
          for (Node node : dataCenter.getNodes()) {
            BoundNode boundNode = (BoundNode) node;
            closeFutures.add(close(boundNode));
          }
        }
        CompletableFuture.allOf(closeFutures.toArray(new CompletableFuture[] {}))
            .whenComplete(
                (__, ex) -> {
                  // remove cluster and complete future.
                  clusters.remove(clusterId);
                  if (ex != null) {
                    future.completeExceptionally(ex);
                  } else {
                    future.complete(foundCluster);
                  }
                });
      } else {
        future.completeExceptionally(new IllegalArgumentException("Cluster not found."));
      }
    }

    return future;
  }

  /**
   * Unregister all currently registered clusters.
   *
   * @return future that is completed when all clusters are unregistered.
   */
  public CompletableFuture<Integer> unregisterAll() {
    List<CompletableFuture<Cluster>> futures =
        clusters.keySet().stream().map(this::unregister).collect(Collectors.toList());

    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[] {}))
        .thenApply(__ -> futures.size());
  }

  /** see {@link #register(Node, ServerOptions)} */
  public CompletableFuture<Node> register(Node node) {
    return register(node, ServerOptions.DEFAULT);
  }

  /**
   * Convenience method that registers a {@link Node} by wrapping it in a 'dummy' {@link Cluster}
   * and registering that.
   *
   * <p>Note that if the given {@link Node} belongs to a {@link Cluster}, the returned future will
   * fail with an {@link IllegalArgumentException}.
   *
   * @param node node to register.
   * @param serverOptions custom server options to use when registering this node.
   * @return A future that when it completes provides an updated {@link Cluster} with ids and
   *     addresses assigned. Note that the return value is not the same object as the input.
   */
  public CompletableFuture<Node> register(Node node, ServerOptions serverOptions) {
    if (node.getDataCenter() != null) {
      CompletableFuture<Node> future = new CompletableFuture<>();
      future.completeExceptionally(
          new IllegalArgumentException("Node belongs to a Cluster, should be standalone."));
      return future;
    }
    // Wrap node in dummy cluster
    Long clusterId = clusterCounter.getAndIncrement();
    Cluster dummyCluster = Cluster.builder().withId(clusterId).withName("dummy").build();
    DataCenter dummyDataCenter = dummyCluster.addDataCenter().withName("dummy").build();

    clusters.put(clusterId, dummyCluster);

    boolean activityLogging =
        serverOptions.isActivityLoggingEnabled() != null
            ? serverOptions.isActivityLoggingEnabled()
            : this.activityLogging;
    return bindInternal(
        node,
        dummyDataCenter,
        node.resolvePeerInfo("token").orElse("0").toString(),
        activityLogging);
  }

  /** @return a map of registered {@link Cluster} instances keyed by their identifier. */
  public Map<Long, Cluster> getClusterRegistry() {
    return this.clusters;
  }

  /** @return The store that configures how to handle requests sent from a client. */
  public StubStore getStubStore() {
    return this.stubStore;
  }

  public Optional<Long> getClusterIdFromIdOrName(String IdOrName) {
    return this.getClusterRegistry()
        .entrySet()
        .stream()
        .filter(
            c ->
                c.getValue().getName().equals(IdOrName)
                    || c.getValue().getId().toString().equals(IdOrName))
        .map(Map.Entry::getValue)
        .findAny()
        .map(AbstractNodeProperties::getId);
  }

  public Optional<Long> getDatacenterIdFromIdOrName(Long clusterId, String IdOrName) {
    return this.getClusterRegistry()
        .get(clusterId)
        .getDataCenters()
        .stream()
        .filter(d -> d.getName().equals(IdOrName) || d.getId().toString().equals(IdOrName))
        .findAny()
        .map(AbstractNodeProperties::getId);
  }

  public Optional<Long> getNodeIdFromIdOrName(Long clusterId, Long datacenterId, String IdOrName) {
    Optional<DataCenter> dc =
        this.getClusterRegistry()
            .get(clusterId)
            .getDataCenters()
            .stream()
            .filter(d -> d.getId().equals(datacenterId))
            .findAny();

    return dc.flatMap(
        d ->
            d.getNodes()
                .stream()
                .filter(n -> n.getName().equals(IdOrName) || n.getId().toString().equals(IdOrName))
                .findAny()
                .map(AbstractNodeProperties::getId));
  }

  private CompletableFuture<Node> bindInternal(
      Node refNode, DataCenter parent, String token, boolean activityLogging) {
    // derive a token for this node. This is done here as the ordering of nodes under a
    // data center is changed when it is bound.
    Map<String, Object> newPeerInfo = new HashMap<>(refNode.getPeerInfo());
    newPeerInfo.put("token", token);
    // Use node's address if set, otherwise generate a new one.
    SocketAddress address =
        refNode.getAddress() != null ? refNode.getAddress() : addressResolver.get();
    CompletableFuture<Node> f = new CompletableFuture<>();
    ChannelFuture bindFuture = this.serverBootstrap.bind(address);
    bindFuture.addListener(
        (ChannelFutureListener)
            channelFuture -> {
              if (channelFuture.isSuccess()) {
                // TODO: Since nodes may be added in a different order when binding than when defined, we need some
                // way of ensuring nodes are added to the new DataCenter in the same order as they were originally
                // defined.
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
                        parent,
                        serverBootstrap,
                        timer,
                        channelFuture.channel(),
                        stubStore,
                        activityLogging);
                logger.info("Bound {} to {}", node, channelFuture.channel());
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

  private CompletableFuture<Node> close(BoundNode node) {
    logger.debug("Closing {}.", node);
    return node.close()
        .thenApply(
            n -> {
              logger.debug(
                  "Releasing {} back to address resolver so it may be reused.", node.getAddress());
              addressResolver.release(n.getAddress());
              return n;
            });
  }

  /**
   * Convenience method that provides a {@link Builder} that uses NIO for networking traffic, which
   * should be the typical case.
   *
   * @return Builder that is set up with {@link NioEventLoopGroup} and {@link
   *     NioServerSocketChannel}.
   */
  public static Builder builder() {
    // Use Epoll or KQueue if available.
    if (Epoll.isAvailable()) {
      logger.debug("Detected epoll support, using EpollEventLoopGroup");
      return builder(new EpollEventLoopGroup(), EpollServerSocketChannel.class);
    } else {
      logger.debug("Could not locate native transport (epoll), using NioEventLoopGroup");
      return builder(new NioEventLoopGroup(), NioServerSocketChannel.class);
    }
  }
  /**
   * Constructs a {@link Builder} that uses the given {@link EventLoopGroup} and {@link
   * ServerChannel} to construct a {@link ServerBootstrap} to be used by this {@link Server}.
   *
   * @param eventLoopGroup event loop group to use.
   * @param channelClass channel class to use, should be compatible with the event loop.
   * @return Builder that is set up with a {@link ServerBootstrap}.
   */
  public static Builder builder(
      EventLoopGroup eventLoopGroup, Class<? extends ServerChannel> channelClass) {
    ServerBootstrap serverBootstrap =
        new ServerBootstrap()
            .group(eventLoopGroup)
            .channel(channelClass)
            .childHandler(new Initializer());
    return new Builder(serverBootstrap);
  }

  public static class Builder {
    private ServerBootstrap serverBootstrap;

    private AddressResolver addressResolver = AddressResolver.defaultResolver;

    private static long DEFAULT_BIND_TIMEOUT_IN_NANOS =
        TimeUnit.NANOSECONDS.convert(10, TimeUnit.SECONDS);

    private long bindTimeoutInNanos = DEFAULT_BIND_TIMEOUT_IN_NANOS;

    private Timer timer;

    private StubStore stubStore;

    private boolean activityLogging = true;

    Builder(ServerBootstrap initialBootstrap) {
      this.serverBootstrap = initialBootstrap;
    }

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
     * Sets the address resolver to use when assigning {@link SocketAddress} to {@link Node}'s that
     * don't have previously provided addresses.
     *
     * @param addressResolver resolver to use.
     * @return This builder.
     */
    public Builder withAddressResolver(AddressResolver addressResolver) {
      this.addressResolver = addressResolver;
      return this;
    }

    /**
     * Sets the timer to use for scheduling actions. If not set, a {@link HashedWheelTimer} is
     * created.
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
      }
      if (timer == null) {
        this.timer = new HashedWheelTimer();
      }
      return new Server(
          addressResolver, serverBootstrap, timer, bindTimeoutInNanos, stubStore, activityLogging);
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
