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
package com.datastax.oss.simulacron.protocol.json;

import com.datastax.oss.protocol.internal.request.Execute;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ExecuteSerializer extends MessageSerializer<Execute> {
  @Override
  public void serializeMessage(
      Execute execute, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeObjectField("id", ByteBuffer.wrap(execute.queryId));
    jsonGenerator.writeObjectField(
        "result_metadata_id",
        execute.resultMetadataId != null ? ByteBuffer.wrap(execute.resultMetadataId) : null);
    jsonGenerator.writeObjectField("options", execute.options);
  }
}
