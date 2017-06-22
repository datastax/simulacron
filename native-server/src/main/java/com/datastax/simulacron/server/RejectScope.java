package com.datastax.simulacron.server;

public enum RejectScope {
  UNBIND, // unbind the channel so can't establish TCP connection
  STOP, // unbind the channel and close all nodes, similar to as if a node had been stopped.
  REJECT_STARTUP // keep channel bound so can establish TCP connection, but doesn't reply to startup.
}
