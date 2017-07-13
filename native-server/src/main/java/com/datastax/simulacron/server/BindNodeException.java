package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.AbstractNode;

import java.net.SocketAddress;

public final class BindNodeException extends Exception {

  private final AbstractNode node;
  private final SocketAddress address;

  public BindNodeException(AbstractNode node, SocketAddress address, Throwable cause) {
    super("Failed to bind " + node + " to " + address, cause);
    this.node = node;
    this.address = address;
  }

  public AbstractNode getNode() {
    return node;
  }

  public SocketAddress getAddress() {
    return address;
  }
}
