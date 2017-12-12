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
package com.datastax.oss.simulacron.server;

import com.datastax.oss.protocol.internal.Compressor;
import com.datastax.oss.protocol.internal.FrameCodec;
import io.netty.buffer.ByteBuf;
import java.util.Set;

class FrameCodecWrapper extends FrameCodec<ByteBuf> {

  private static final ByteBufCodec codec = new ByteBufCodec();

  private final Set<Integer> supportedProtocolVersions;

  FrameCodecWrapper(Set<Integer> supportedProtocolVersions, CodecGroup... codecGroups) {
    super(codec, Compressor.none(), codecGroups);
    this.supportedProtocolVersions = supportedProtocolVersions;
  }

  Set<Integer> getSupportedProtocolVersions() {
    return supportedProtocolVersions;
  }
}
