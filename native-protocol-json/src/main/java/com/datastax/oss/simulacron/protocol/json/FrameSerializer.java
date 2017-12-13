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
package com.datastax.oss.simulacron.protocol.json;

import com.datastax.oss.protocol.internal.Frame;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class FrameSerializer extends JsonSerializer<Frame> {
  @Override
  public void serialize(
      Frame frame, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject(frame);
    jsonGenerator.writeObjectField("protocol_version", frame.protocolVersion);
    jsonGenerator.writeObjectField("beta", frame.beta);
    jsonGenerator.writeObjectField("stream_id", frame.streamId);
    jsonGenerator.writeObjectField("tracing_id", frame.tracing ? frame.tracingId.toString() : null);
    jsonGenerator.writeObjectField("custom_payload", frame.customPayload);
    jsonGenerator.writeObjectField("warnings", frame.warnings);
    jsonGenerator.writeObjectField("message", frame.message);
    jsonGenerator.writeEndObject();
  }
}
