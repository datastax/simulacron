package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.UUID;

public abstract class AbstractNodeProperties implements NodeProperties {

  private final String name;
  private final UUID id;

  @JsonProperty("cassandra_version")
  private final String cassandraVersion;

  @JsonProperty("peer_info")
  private final Map<String, Object> peerInfo;

  AbstractNodeProperties(
      String name, UUID id, String cassandraVersion, Map<String, Object> peerInfo) {
    this.name = name;
    this.id = id;
    this.cassandraVersion = cassandraVersion;
    this.peerInfo = peerInfo;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public UUID getId() {
    return id;
  }

  @Override
  public String getCassandraVersion() {
    return cassandraVersion;
  }

  @Override
  public Map<String, Object> getPeerInfo() {
    return peerInfo;
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
    if (!peerInfo.isEmpty()) {
      str.append(", peerInfo=" + peerInfo);
    }
    str.append(extras);
    str.append("}");
    return str.toString();
  }
}
