package com.datastax.simulacron.server.token;

import com.datastax.simulacron.common.cluster.NodeSpec;
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
