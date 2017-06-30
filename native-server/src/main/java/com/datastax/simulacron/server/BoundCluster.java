package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.Cluster;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A wrapper around {@link Cluster} that is bound to a {@link Server}. If used as {@link
 * java.io.Closeable} will unbind itself form its bound server.
 */
class BoundCluster extends Cluster {

  private final Server server;

  private final AtomicBoolean unregistered = new AtomicBoolean();

  BoundCluster(Cluster delegate, Long clusterId, Server server) {
    super(
        delegate.getName(),
        clusterId,
        delegate.getCassandraVersion(),
        delegate.getDSEVersion(),
        delegate.getPeerInfo());
    this.server = server;
  }

  @Override
  public void close() throws IOException {
    // only unregister if hasn't been previously unregistered.
    if (unregistered.compareAndSet(false, true)) {
      // cluster may have been unregistered manually, in this case
      // it will be absent from the registry.
      if (server.getClusterRegistry().containsKey(this.getId())) {
        server.unregister(this).join();
      }
    }
  }
}
