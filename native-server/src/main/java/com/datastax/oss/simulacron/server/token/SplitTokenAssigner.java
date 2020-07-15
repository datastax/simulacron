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
package com.datastax.oss.simulacron.server.token;

import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
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
