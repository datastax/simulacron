package com.datastax.simulacron.cluster;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;

public abstract class AbstractNodeProperties implements NodeProperties {

  private final String name;
  private final UUID id;
  private final String cassandraVersion;
  private final Map<String, Object> peerInfo;
  private final NodeProperties parent;

  AbstractNodeProperties(
      String name,
      UUID id,
      String cassandraVersion,
      Map<String, Object> peerInfo,
      NodeProperties parent) {
    this.name = name;
    this.id = id;
    this.cassandraVersion = cassandraVersion;
    this.peerInfo = peerInfo;
    this.parent = parent;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public UUID id() {
    return id;
  }

  @Override
  public String cassandraVersion() {
    return cassandraVersion;
  }

  @Override
  public Map<String, Object> peerInfo() {
    return peerInfo;
  }

  @Override
  public Optional<NodeProperties> parent() {
    return Optional.ofNullable(parent);
  }

  String toStringWith(String extras) {
    StringBuilder str = new StringBuilder(this.getClass().getSimpleName());
    str.append("{");
    str.append("id=" + id);
    if (!name.equals(id.toString())) {
      str.append(", name='" + name + '\'');
    }
    if (cassandraVersion != null) {
      str.append(", cassandraVersion='" + cassandraVersion + '\'');
    }
    if (!peerInfo.isEmpty()) {
      str.append(", peerInfo=" + peerInfo);
    }
    if (parent != null) {
      if (parent.name().equals(parent.id().toString())) {
        str.append(", parent=" + parent.resolveId());
      } else {
        str.append(", parent=" + parent.resolveName());
      }
    }
    str.append(extras);
    str.append("}");
    return str.toString();
  }
}
