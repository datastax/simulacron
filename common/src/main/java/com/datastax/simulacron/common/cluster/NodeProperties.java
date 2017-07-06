package com.datastax.simulacron.common.cluster;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static java.util.Optional.ofNullable;

/**
 * Interface that defines an Object having a name, id, cassandra version a mapping of peer info and
 * optionally a parent which also defines these properties.
 *
 * <p>The main utility for this is to provide a mechanism to resolve the "most specific" value to
 * use for a node. For example, if you have a node belonging to a datacenter which belongs a
 * cluster, one can define a cassandraVersion at the cluster level and calls to {@link
 * #resolveCassandraVersion()} on the node will return the cassandraVersion defined at the node
 * level if defined, otherwise data center, otherwise cluster, otherwise some default value.
 */
public interface NodeProperties extends Comparable<NodeProperties> {

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
        .orElseGet(
            () -> getParent().map(p -> p.resolve(methodRef, defaultValue)).orElse(defaultValue));
  }

  /** @return A human readable name for this. */
  String getName();

  /** @return A unique id for this. */
  Long getId();

  /**
   * @return The cassandra version of this if set, otherwise null. The cassandra version is used to
   *     determine formatting of schema tables and maybe eventually other behavior.
   */
  String getCassandraVersion();

  /**
   * @return The DSE version of this if set, otherwise null. THe dse version is used to determine
   *     formatting of system and schema tables.
   */
  String getDSEVersion();

  /**
   * @return The peer info of this, should always be nonnull. The peer info is a mapping of column
   *     names to values to be used in the system.peers and local table.
   */
  Map<String, Object> getPeerInfo();

  /** @return The number of active connections on a node */
  Long getActiveConnections();

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

  /** @return the dse version for this, otherwise its parents. If it is not set, null is used. */
  default String resolveDSEVersion() {
    return resolve(NodeProperties::getDSEVersion, null);
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
    return getParent().map(p -> p.resolveId() + ":").orElse("") + getId();
  }

  /**
   * @return {@link NodeProperties#getName()} for this, and if there are parents, prefixes parent's
   *     name(s) separated by '/', otherwise returns.
   */
  default String resolveIdPath() {
    return getParent().map(p -> p.resolveIdPath() + "/").orElse("") + getId();
  }

  /**
   * @param key peer column name
   * @return the value for this peer column for this if set, otherwise its parents. If it is not
   *     set, Optional.empty is returned.
   */
  default <T> Optional<T> resolvePeerInfo(String key, Class<T> clazz) {
    // if value is present, return it, otherwise try parent.
    if (!getPeerInfo().containsKey(key)) {
      return getParent().flatMap(p -> p.resolvePeerInfo(key, clazz));
    }
    Object value = getPeerInfo().get(key);
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(clazz.cast(value));
    } catch (ClassCastException e) {
      return Optional.empty();
    }
  }

  /**
   * @param key peer column name
   * @return Whether or not peer info is present for the given key. This is useful for explicit
   *     values being set to null.
   */
  default boolean isPeerInfoPresent(String key) {
    if (!getPeerInfo().containsKey(key)) {
      return getParent().map(p -> p.isPeerInfoPresent(key)).orElse(false);
    } else {
      return true;
    }
  }

  /**
   * @param key peer column name
   * @param defaultValue the value to return if this or none of its parents have a mapping for the
   *     given key.
   * @return the value for this peer column for this if set, otherwise its parents. If it is not
   *     set, defaultValue is returned.
   */
  @SuppressWarnings("unchecked")
  default <T> T resolvePeerInfo(String key, T defaultValue) {
    Optional<T> val = resolvePeerInfo(key, (Class<T>) defaultValue.getClass());
    if (val.isPresent()) {
      return val.get();
    } else {
      // property is set, but it's null, return that.
      if (isPeerInfoPresent(key)) {
        return null;
      } else {
        return defaultValue;
      }
    }
  }

  @Override
  default int compareTo(NodeProperties other) {
    // by default compare by id, this is not perfect if comparing cross-dc or comparing different types but in the
    // general case this should only be used for comparing for things in same group.
    return (int) (this.getId() - other.getId());
  }
}
