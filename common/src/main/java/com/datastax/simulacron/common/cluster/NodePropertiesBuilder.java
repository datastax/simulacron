package com.datastax.simulacron.common.cluster;

import java.util.HashMap;
import java.util.Map;

public abstract class NodePropertiesBuilder<
    S extends NodePropertiesBuilder<S, P>, P extends NodeProperties> {

  P parent;
  private final S myself;
  String cassandraVersion;
  String name;
  Long id;
  Map<String, Object> peerInfo = new HashMap<>();

  NodePropertiesBuilder(Class<?> selfType) {
    this(selfType, null);
  }

  @SuppressWarnings("unchecked")
  NodePropertiesBuilder(Class<?> selfType, P parent) {
    this.myself = (S) selfType.cast(this);
    this.parent = parent;
  }

  /**
   * Copies properties from a previously built {@link NodeProperties} into this. This is useful for
   * when we are creating a new copy that has new fields set like ID for example.
   *
   * @param toCopy Existing {@link NodeProperties} toCopy.
   * @return Updated builder with copied properties.
   */
  public S copy(NodeProperties toCopy) {
    return this.withCassandraVersion(toCopy.getCassandraVersion())
        .withName(toCopy.getName())
        .withId(toCopy.getId())
        .withPeerInfo(toCopy.getPeerInfo());
  }

  public S withCassandraVersion(String cassandraVersion) {
    this.cassandraVersion = cassandraVersion;
    return myself;
  }

  public S withName(String name) {
    this.name = name;
    return myself;
  }

  public S withId(Long id) {
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
