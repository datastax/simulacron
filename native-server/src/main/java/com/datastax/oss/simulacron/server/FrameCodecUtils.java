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

import com.datastax.oss.protocol.internal.FrameCodec;
import com.datastax.oss.protocol.internal.ProtocolV3ServerCodecs;
import com.datastax.oss.protocol.internal.ProtocolV4ServerCodecs;
import com.datastax.oss.protocol.internal.ProtocolV5ServerCodecs;
import com.datastax.oss.simulacron.common.cluster.NodeProperties;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class FrameCodecUtils {

  private static final Logger logger = LoggerFactory.getLogger(FrameCodecUtils.class);
  private static final List<Integer> defaultProtocolVersions = new ArrayList<>();
  private static final String PROTOCOL_VERSIONS = "protocol_versions";

  static {
    defaultProtocolVersions.add(3);
    defaultProtocolVersions.add(4);
  }

  private static final FrameCodecWrapper defaultFrameCodec =
      new FrameCodecWrapper(
          new TreeSet<>(defaultProtocolVersions),
          new ProtocolV3ServerCodecs(),
          new ProtocolV4ServerCodecs());

  static Optional<FrameCodecWrapper> buildFrameCodec(NodeProperties topic) {
    // First attempt to parse "protocol_versions" from peer info, if present use those.
    if (topic.getPeerInfo().containsKey(PROTOCOL_VERSIONS)) {
      List<Integer> protocolVersions =
          topic.resolvePeerInfo(PROTOCOL_VERSIONS, defaultProtocolVersions);
      return Optional.of(buildFrameCodec(new TreeSet<>(protocolVersions)));
    } else {
      // Next attempt to resolve protocol versions from configured cassandra version but only do
      // this if protocol_versions does not resolve at all.
      if (topic.getCassandraVersion() != null && !topic.isPeerInfoPresent(PROTOCOL_VERSIONS)) {
        if (topic.getCassandraVersion().startsWith("4.")) {
          return Optional.of(buildFrameCodec(3, 4, 5));
        } else if (topic.getCassandraVersion().startsWith("3.0")
            || topic.getCassandraVersion().startsWith("2.2")) {
          return Optional.of(buildFrameCodec(3, 4));
        } else if (topic.getCassandraVersion().startsWith("2.1")) {
          return Optional.of(buildFrameCodec(3));
        } else {
          logger.warn(
              "Could not resolve supported protocol versions from cassandra version "
                  + topic.getCassandraVersion());
        }
      }
      // If can't be resolved, return empty, caller will likely defer to parent or use
      // defaultFrameCodec.
      return Optional.empty();
    }
  }

  static FrameCodecWrapper defaultFrameCodec() {
    return defaultFrameCodec;
  }

  static FrameCodecWrapper buildFrameCodec(Integer... protocolVersions) {
    Set<Integer> versions = new TreeSet<>(Arrays.asList(protocolVersions));
    return buildFrameCodec(versions);
  }

  static FrameCodecWrapper buildFrameCodec(Set<Integer> protocolVersions) {
    if (protocolVersions.equals(defaultProtocolVersions)) {
      return defaultFrameCodec;
    }
    List<FrameCodec.CodecGroup> codecGroups = new ArrayList<>();
    for (int protocolVersion : protocolVersions) {
      switch (protocolVersion) {
        case 3:
          codecGroups.add(new ProtocolV3ServerCodecs());
          break;
        case 4:
          codecGroups.add(new ProtocolV4ServerCodecs());
          break;
        case 5:
          codecGroups.add(new ProtocolV5ServerCodecs());
          break;
        default:
          logger.warn("Unknown protocol versions '{}' provided, ignoring.", protocolVersion);
      }
    }
    return new FrameCodecWrapper(
        protocolVersions, codecGroups.toArray(new FrameCodec.CodecGroup[codecGroups.size()]));
  }
}
