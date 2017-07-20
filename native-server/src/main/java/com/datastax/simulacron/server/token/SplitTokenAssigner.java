package com.datastax.simulacron.server.token;

import com.datastax.simulacron.common.cluster.ClusterSpec;
import com.datastax.simulacron.common.cluster.DataCenterSpec;
import com.datastax.simulacron.common.cluster.NodeSpec;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.Map;

/** Creates tokens to nodes by splitting the ring evenly */
public class SplitTokenAssigner extends TokenAssigner {

  /** Map with node and token * */
  private Map<String, String> tokens = new HashMap<>();

  public SplitTokenAssigner(ClusterSpec clusterSpec) {
    createTokens(clusterSpec);
  }

  private void createTokens(ClusterSpec clusterSpec) {
    int dcPos = 0;
    int nPos = 0;
    for (DataCenterSpec dataCenter : clusterSpec.getDataCenters()) {
      long dcOffset = dcPos * 100;
      BigInteger nodeBaseBig =
          BigInteger.valueOf(2).pow(64).divide(BigInteger.valueOf(dataCenter.getNodes().size()));
      long nodeBase = nodeBaseBig.longValue();

      for (NodeSpec node : dataCenter.getNodes()) {
        long nodeOffset = Long.MIN_VALUE + (nPos * nodeBase);
        String tokenStr = "" + (nodeOffset + dcOffset);
        tokens.put(node.resolveId(), tokenStr);
        nPos++;
      }
      dcPos++;
    }
  }

  @Override
  String getTokensInternal(NodeSpec nodeSpec) {
    return tokens.get(nodeSpec.resolveId());
  }
}
