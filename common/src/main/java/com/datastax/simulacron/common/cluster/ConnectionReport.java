package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.net.SocketAddress;
import java.util.List;

public abstract class ConnectionReport extends AbstractIdentifiable {

  ConnectionReport(Long id) {
    super(id);
  }

  @JsonIgnore
  public abstract ClusterConnectionReport getRootReport();

  @JsonIgnore
  public abstract List<SocketAddress> getConnections();
}
