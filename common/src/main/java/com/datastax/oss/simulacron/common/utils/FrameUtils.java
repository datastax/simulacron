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
package com.datastax.oss.simulacron.common.utils;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class FrameUtils {

  public static final Map<String, ByteBuffer> emptyCustomPayload =
      Collections.unmodifiableMap(new HashMap<>());

  public static Frame wrapResponse(Frame requestFrame, Message response) {
    return new Frame(
        requestFrame.protocolVersion,
        requestFrame.beta,
        requestFrame.streamId,
        false,
        null,
        -1,
        -1,
        emptyCustomPayload,
        Collections.emptyList(),
        response);
  }

  public static Frame wrapRequest(Message message) {
    return new Frame(
        4, false, 0, false, null, -1, -1, Collections.emptyMap(), Collections.emptyList(), message);
  }

  public static Frame convertResponseMessage(Frame responseFrame, Message newMessage) {
    return new Frame(
        responseFrame.protocolVersion,
        responseFrame.beta,
        responseFrame.streamId,
        responseFrame.tracing,
        responseFrame.tracingId,
        -1,
        -1,
        Frame.NO_PAYLOAD,
        Collections.emptyList(),
        newMessage);
  }
}
