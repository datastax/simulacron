package com.datastax.simulacron.server;

import com.datastax.simulacron.common.cluster.Node;

import java.net.SocketAddress;

public final class BindNodeException extends Exception {

  private final Node node;
  private final SocketAddress address;

  BindNodeException(Node node, SocketAddress address, Throwable cause) {
    super("Failed to bind " + node + " to " + address, cause);
    this.node = node;
    this.address = address;
  }

  public Node getNode() {
    return node;
  }

  public SocketAddress getAddress() {
    return address;
  }
}
