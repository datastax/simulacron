package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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

  private final transient AtomicReference<Scope> scope = new AtomicReference<Scope>(null);

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

  @Override
  @JsonIgnore
  public Scope getScope() {
    if (scope.get() == null) {
      scope.compareAndSet(null, Scope.scope(this));
    }
    return scope.get();
  }

  @Override
  @JsonIgnore
  public List<QueryLog> getLogs() {
    return getCluster().getActivityLog().getLogs(this);
  }

  @Override
  public void clearLogs() {
    getCluster().getActivityLog().clearLogs(this);
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
