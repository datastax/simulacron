package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;

public abstract class ConnectionReport extends AbstractNodeProperties {

  ConnectionReport(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
  }

  @JsonIgnore
  public abstract ClusterConnectionReport getRootReport();
}
