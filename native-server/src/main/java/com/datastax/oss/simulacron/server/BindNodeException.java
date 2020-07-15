/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.simulacron.server;

import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import java.net.SocketAddress;

public final class BindNodeException extends Exception {

  private final AbstractNode node;
  private final SocketAddress address;

  public BindNodeException(AbstractNode node, SocketAddress address, Throwable cause) {
    super("Failed to bind " + node + " to " + address, cause);
    this.node = node;
    this.address = address;
  }

  public AbstractNode getNode() {
    return node;
  }

  public SocketAddress getAddress() {
    return address;
  }
}
