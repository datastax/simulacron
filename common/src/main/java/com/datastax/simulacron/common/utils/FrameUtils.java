package com.datastax.simulacron.common.utils;

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
        emptyCustomPayload,
        Collections.emptyList(),
        response);
  }

  public static Frame wrapRequest(Message message) {
    return new Frame(
        4, false, 0, false, null, Collections.emptyMap(), Collections.emptyList(), message);
  }
}
