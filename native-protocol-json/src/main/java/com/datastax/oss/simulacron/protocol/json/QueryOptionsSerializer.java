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

import static com.datastax.oss.simulacron.protocol.json.SerializerUtils.toConsistencyString;

import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;

public class QueryOptionsSerializer extends JsonSerializer<QueryOptions> {
  @Override
  public void serialize(
      QueryOptions queryOptions, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
      throws IOException {
    jsonGenerator.writeStartObject(queryOptions);
    jsonGenerator.writeObjectField("consistency", toConsistencyString(queryOptions.consistency));
    jsonGenerator.writeObjectField("positional_values", queryOptions.positionalValues);
    jsonGenerator.writeObjectField("named_values", queryOptions.namedValues);
    jsonGenerator.writeObjectField("skip_metadata", queryOptions.skipMetadata);
    jsonGenerator.writeObjectField("page_size", queryOptions.pageSize);
    jsonGenerator.writeObjectField("paging_state", queryOptions.pagingState);
    jsonGenerator.writeObjectField(
        "serial_consistency", toConsistencyString(queryOptions.serialConsistency));
    jsonGenerator.writeObjectField("default_timestamp", queryOptions.defaultTimestamp);
    jsonGenerator.writeObjectField("keyspace", queryOptions.keyspace);
    jsonGenerator.writeEndObject();
  }
}
