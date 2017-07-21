/*
 * Copyright (C) 2017-2017 DataStax Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonBackReference;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Optional;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

public class AbstractNode<C extends AbstractCluster, D extends AbstractDataCenter<C, ?>>
    extends AbstractNodeProperties implements NodeStructure<C, D> {

  /** The address and port that this node should listen on. */
  @JsonProperty
  @JsonInclude(NON_NULL)
  private final SocketAddress address;

  @JsonBackReference private final D parent;

  public AbstractNode(
      SocketAddress address,
      String name,
      Long id,
      String cassandraVersion,
      String dseVersion,
      Map<String, Object> peerInfo,
      D parent) {
    super(name, id, cassandraVersion, dseVersion, peerInfo);
    this.address = address;
    this.parent = parent;
    if (this.parent != null) {
      parent.addNode(this);
    }
  }

  /** @return The address and port that this node should listen on. */
  public SocketAddress getAddress() {
    return address;
  }

  /**
   * Convenience method to get access {@link #getAddress()} as {@link InetSocketAddress} instance
   * which it almost always is (except in testing scenarios.
   *
   * @return address as an {@link InetSocketAddress}
   */
  public InetSocketAddress inetSocketAddress() {
    if (address instanceof InetSocketAddress) {
      return (InetSocketAddress) address;
    }
    return null;
  }

  /**
   * Convenience method to access the {@link InetAddress} part of {@link #getAddress()} if it is an
   * {@link InetSocketAddress}.
   *
   * @return address as an {@link InetAddress}
   */
  public InetAddress inet() {
    InetSocketAddress addr = inetSocketAddress();
    if (addr != null) {
      return addr.getAddress();
    }
    return null;
  }

  /**
   * Convenience method to access the port part of {@link #getAddress()} if it is an {@link
   * InetSocketAddress}.
   *
   * @return port
   */
  public int port() {
    InetSocketAddress addr = inetSocketAddress();
    if (addr != null) {
      return addr.getPort();
    }
    return -1;
  }

  @Override
  public D getDataCenter() {
    return parent;
  }

  @Override
  public C getCluster() {
    if (parent != null) {
      return parent.getCluster();
    }
    return null;
  }

  @Override
  public String toString() {
    return toStringWith(", address=" + address);
  }

  @Override
  @JsonIgnore
  public Optional<NodeProperties> getParent() {
    return Optional.ofNullable(parent);
  }

  @Override
  public Long getActiveConnections() {
    // In the case of a concrete 'NodeSpec' instance, active connections will always be 0 since there is no actual
    // connection state here.  We expect specialized implementations of NodeSpec to override this.
    return 0L;
  }
}
