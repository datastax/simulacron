package com.datastax.simulacron.common.cluster;

import java.util.HashMap;
import java.util.Map;

/**
 * A base builder to use when defining a {@link NodeProperties} implementation that provides builder
 * methods for version, name, id, and peer info. Also provides a {@link #copy(NodeProperties)}
 * method for deriving {@link NodeProperties} values from an existing object.
 *
 * @param <S> The concrete type of the builder.
 * @param <N> The type of properties being created.
 */
public abstract class NodePropertiesBuilder<
    S extends NodePropertiesBuilder<S, N>, N extends NodeProperties> {

  N parent;
  private final S myself;
  String cassandraVersion;
  String dseVersion;
  String name;
  Long id;
  Map<String, Object> peerInfo = new HashMap<>();

  NodePropertiesBuilder(Class<?> selfType) {
    this(selfType, null);
  }

  @SuppressWarnings("unchecked")
  NodePropertiesBuilder(Class<?> selfType, N parent) {
    this.myself = (S) selfType.cast(this);
    this.parent = parent;
  }

  /**
   * Copies properties from a previously built {@link NodeProperties} into this. This is useful for
   * when we are creating a new copy that has new fields set like ID for example.
   *
   * <p>Note that this doesn't copy values like children or environment specific properties (like
   * address).
   *
   * @param toCopy Existing {@link NodeProperties} toCopy.
   * @return Updated builder with copied properties.
   */
  public S copy(NodeProperties toCopy) {
    return this.withCassandraVersion(toCopy.getCassandraVersion())
        .withDSEVersion(toCopy.getDSEVersion())
        .withName(toCopy.getName())
        .withId(toCopy.getId())
        .withPeerInfo(toCopy.getPeerInfo());
  }

  public S withCassandraVersion(String cassandraVersion) {
    this.cassandraVersion = cassandraVersion;
    return myself;
  }

  public S withDSEVersion(String dseVersion) {
    this.dseVersion = dseVersion;
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
    this.peerInfo = new HashMap<>(peerInfo);
    return myself;
  }
}
