package com.datastax.simulacron.common.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public abstract class NodePropertiesBuilder<
    S extends NodePropertiesBuilder<S, P>, P extends NodeProperties> {

  P parent;
  private final S myself;
  String cassandraVersion;
  String name;
  UUID id;
  Map<String, Object> peerInfo = new HashMap<>();

  NodePropertiesBuilder(Class<?> selfType) {
    this(selfType, null);
  }

  @SuppressWarnings("unchecked")
  NodePropertiesBuilder(Class<?> selfType, P parent) {
    this.myself = (S) selfType.cast(this);
    this.parent = parent;
  }

  public S withCassandraVersion(String cassandraVersion) {
    this.cassandraVersion = cassandraVersion;
    return myself;
  }

  public S withName(String name) {
    this.name = name;
    return myself;
  }

  public S withId(UUID id) {
    this.id = id;
    return myself;
  }

  public S withPeerInfo(String key, Object value) {
    peerInfo.put(key, value);
    return myself;
  }

  public S withPeerInfo(Map<String, Object> peerInfo) {
    this.peerInfo = peerInfo;
    return myself;
  }
}
