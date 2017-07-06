package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

/**
 * Implementation of {@link NodeProperties} that provides a constructor a implementations for
 * everything except {@link NodeProperties#getParent()}.
 */
// Mark active_connections ignorable as they are not used on serialization.
@JsonIgnoreProperties(
  value = {"active_connections"},
  allowGetters = true
)
public abstract class AbstractNodeProperties implements NodeProperties {

  private final String name;
  private final Long id;

  @JsonProperty("cassandra_version")
  private final String cassandraVersion;

  @JsonProperty("dse_version")
  private final String dseVersion;

  @JsonProperty("peer_info")
  private final Map<String, Object> peerInfo;

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
  @JsonProperty("active_connections")
  public abstract Long getActiveConnections();

  @JsonIgnore
  public abstract Cluster getCluster();

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
