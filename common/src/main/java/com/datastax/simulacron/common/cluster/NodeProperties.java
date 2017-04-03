package com.datastax.simulacron.common.cluster;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

public interface NodeProperties {

  /**
   * Convenience utility such that for a given method reference of an instance method of {@link
   * NodeProperties} invokes that method first on the instance. If that method returns null, it then
   * calls the parent instance. If the parent instance method returns null, it then returns the
   * default value.
   *
   * <p>
   *
   * <p>Example use case:
   *
   * <p>
   *
   * <p>A cluster is defined with an ipPrefix of '127.0.0.', but this is not set at the datacenter
   * or node level. If one were to call {@link NodeProperties::resolveIpPrefix()}, it would first
   * check the node's ipPrefix, which returns null, then datacenter's ipPrefix, which returns null,
   * and then finally cluster's ipPrefix, which returns a value.
   *
   * @param methodRef The instance method to inspect on this instance and potentially its parents.
   * @param defaultValue The default value to use if neither the instance or its parent's return a
   *     value.
   * @param <T> The type to return.
   * @return The resolved value, or the default value if null.
   */
  default <T> T resolve(Function<NodeProperties, T> methodRef, T defaultValue) {
    return ofNullable(methodRef.apply(this))
        .orElseGet(() -> getParent().map(methodRef).orElse(defaultValue));
  }

  /** @return A human readable name for this. */
  String getName();

  /** @return A unique id for this. */
  UUID getId();

  /**
   * @return The cassandra version of this if set, otherwise null. The cassandra version is used to
   *     determine formatting of schema tables and maybe eventually other behavior.
   */
  String getCassandraVersion();

  /**
   * @return The peer info of this, should always be nonnull. The peer info is a mapping of column
   *     names to values to be used in the system.peers and local table.
   */
  Map<String, Object> getPeerInfo();

  /**
   * @return The parent instance of this. For example, it is expected that a node has a parent data
   *     center, a data center has a parent cluster.
   */
  Optional<NodeProperties> getParent();

  /**
   * @return the cassandra version for this, otherwise its parents. If it is not set, the default
   *     value is used.
   */
  default String resolveCassandraVersion() {
    return resolve(NodeProperties::getCassandraVersion, "3.0.12");
  }

  /**
   * @return {@link NodeProperties#getName()} for this, and if there are parents, prefixes parent's
   *     name(s) separated by ':', otherwise returns.
   */
  default String resolveName() {
    return getParent().map(p -> p.resolveName() + ":").orElse("") + getName();
  }

  /**
   * @return {@link NodeProperties#getName()} for this, and if there are parents, prefixes parent's
   *     name(s) separated by ':', otherwise returns.
   */
  default String resolveId() {
    return getParent().map(p -> p.resolveId() + ":").orElse("") + getId().toString();
  }

  /**
   * @param key peer column name
   * @return the value for this peer column for this if set, otherwise its parents. If it is not
   *     set, Optional.empty is returned.
   */
  default Optional<Object> resolvePeerInfo(String key) {
    // if value is present, return it, otherwise try parent.
    Object value = getPeerInfo().get(key);
    return value != null ? Optional.of(value) : getParent().map(p -> resolvePeerInfo(key));
  }

  /**
   * @param key peer column name
   * @param defaultValue the value to return if this or none of its parents have a mapping for the
   *     given key.
   * @return the value for this peer column for this if set, otherwise its parents. If it is not
   *     set, defaultValue is returned.
   */
  default Object resolvePeerInfo(String key, Object defaultValue) {
    return resolvePeerInfo(key).orElse(defaultValue);
  }
}
