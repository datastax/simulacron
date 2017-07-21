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
package com.datastax.oss.simulacron.server.token;

import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import java.util.Optional;

/** Abstract class with behavior to create tokens for nodes in a cluster */
public abstract class TokenAssigner {

  /**
   * Get tokens to be assigned to node.
   *
   * @param nodeSpec node to retrieve tokens for.
   * @return Comma separated string of token(s)
   */
  public String getTokens(NodeSpec nodeSpec) {
    Optional<String> token = nodeSpec.resolvePeerInfo("tokens", String.class);
    String tokenStr;
    if (token.isPresent()) {
      tokenStr = token.get();
    } else if (nodeSpec.getCluster() == null) {
      tokenStr = "0";
    } else {
      tokenStr = getTokensInternal(nodeSpec);
    }
    return tokenStr;
  }

  /**
   * Abstract method to implement how the tokens will be generated for a given node.
   *
   * @param nodeSpec node to retrieve tokens for.
   * @return Comma separated string of token(s)
   */
  abstract String getTokensInternal(NodeSpec nodeSpec);
}
