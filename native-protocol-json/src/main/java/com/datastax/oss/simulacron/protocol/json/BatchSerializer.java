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

import static com.datastax.oss.simulacron.protocol.json.SerializerUtils.toConsistencyString;

import com.datastax.oss.protocol.internal.request.Batch;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class BatchSerializer extends MessageSerializer<Batch> {
  @Override
  public void serializeMessage(
      Batch batch, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {

    final String batchType;
    switch (batch.type) {
      case 0x0:
        batchType = "LOGGED";
        break;
      case 0x1:
        batchType = "UNLOGGED";
        break;
      case 0x2:
        batchType = "COUNTER";
        break;
      default:
        batchType = "" + batch.type;
    }

    jsonGenerator.writeObjectField("type", batchType);
    jsonGenerator.writeObjectField("queries_or_ids", batch.queriesOrIds);
    jsonGenerator.writeObjectField("values", batch.values);
    jsonGenerator.writeObjectField("consistency", toConsistencyString(batch.consistency));
    jsonGenerator.writeObjectField(
        "serial_consistency", toConsistencyString(batch.serialConsistency));
    jsonGenerator.writeObjectField("default_timestamp", batch.defaultTimestamp);
    jsonGenerator.writeObjectField("keyspace", batch.keyspace);
  }
}
