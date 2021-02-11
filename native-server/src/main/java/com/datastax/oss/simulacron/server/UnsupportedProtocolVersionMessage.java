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

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.util.Collections;

public class UnsupportedProtocolVersionMessage {

  private final int protocolVersion;

  private final short streamId;

  public UnsupportedProtocolVersionMessage(int protocolVersion, short streamId) {
    this.protocolVersion = protocolVersion;
    this.streamId = streamId;
  }

  public int getProtocolVersion() {
    return this.protocolVersion;
  }

  public short getStreamId() {
    return this.streamId;
  }

  /**
   * Returns a faked STARTUP frame with the tried protocol version and stream id. Because we can't
   * precisely infer the message, we just assume it was a STARTUP.
   *
   * @return A fake STARTUP frame with the tried protocol version and stream id.
   */
  public Frame getFrame() {
    return new Frame(
        protocolVersion,
        false,
        streamId,
        false,
        null,
        -1,
        -1,
        FrameUtils.emptyCustomPayload,
        Collections.singletonList(
            "This message contains a non-supported protocol version by this node.  STARTUP is inferred, but may not reflect the actual message sent."),
        new Startup());
  }
}
