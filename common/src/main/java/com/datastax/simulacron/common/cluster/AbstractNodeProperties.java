package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implementation of {@link NodeProperties} that provides a constructor a implementations for
 * everything except {@link NodeProperties#getParent()}.
 */
public abstract class AbstractNodeProperties implements NodeProperties {

  private final String name;
  private final Long id;

  @JsonProperty("cassandra_version")
  private final String cassandraVersion;

  @JsonProperty("dse_version")
  private final String dseVersion;

  @JsonProperty("peer_info")
  private final Map<String, Object> peerInfo;

  @JsonProperty("active_connections")
  private final transient AtomicLong activeConnections = new AtomicLong(0);

  AbstractNodeProperties(
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo) {
    this.name = name;
    this.id = id;
    this.cassandraVersion = cassandraVersion;
    this.dseVersion = dseVersion;
    this.peerInfo = peerInfo;
  }

  @Override
  public String getName() {
    return name != null ? name : id != null ? id.toString() : null;
  }

  @Override
  public Long getId() {
    return id;
  }

  @Override
  public String getCassandraVersion() {
    return cassandraVersion;
  }

  @Override
  @JsonProperty("dse_version")
  public String getDSEVersion() {
    return dseVersion;
  }

  @Override
  public Map<String, Object> getPeerInfo() {
    return peerInfo;
  }

  @Override
  public Long getActiveConnections() {
    return activeConnections.get();
  }

  @Override
  public Long incrementActiveConnections() {
    return activeConnections.incrementAndGet();
  }

  @Override
  public Long decrementActiveConnections() throws Exception {
    Long newValue = activeConnections.decrementAndGet();
    if (newValue >= 0) {
      return newValue;
    } else {
      throw new Exception("Active connections dropped below zero");
    }
  }

  String toStringWith(String extras) {
    StringBuilder str = new StringBuilder(this.getClass().getSimpleName());
    str.append("{");
    if (id != null) {
      str.append("id=" + id);
      if (name != null && !name.equals(id.toString())) {
        str.append(", name='" + name + '\'');
      }
    } else {
      str.append("name='" + name + '\'');
    }

    if (cassandraVersion != null) {
      str.append(", cassandraVersion='" + cassandraVersion + '\'');
    }
    if (dseVersion != null) {
      str.append(", dseVersion='" + dseVersion + '\'');
    }
    if (!peerInfo.isEmpty()) {
      str.append(", peerInfo=" + peerInfo);
    }
    str.append(extras);
    str.append("}");
    return str.toString();
  }
}
