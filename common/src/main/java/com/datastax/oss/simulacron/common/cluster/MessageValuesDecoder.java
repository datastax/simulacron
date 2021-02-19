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
package com.datastax.oss.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.simulacron.common.codec.Codec;
import com.datastax.oss.simulacron.common.codec.CodecUtils;
import com.datastax.oss.simulacron.common.codec.CqlMapper;
import com.datastax.oss.simulacron.common.request.Batch;
import com.datastax.oss.simulacron.common.request.Statement;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

class MessageValuesDecoder {

  public static List<LinkedHashMap<String, Object>> decode(Prime prime, Frame frame) {

    List<LinkedHashMap<String, Object>> result = new ArrayList<>();
    RequestPrime primedRequest = prime.getPrimedRequest();

    if (primedRequest.when instanceof com.datastax.oss.simulacron.common.request.Query) {

      Map<String, String> paramTypes =
          ((com.datastax.oss.simulacron.common.request.Query) primedRequest.when).paramTypes;
      if (paramTypes != null) {
        if (frame.message instanceof Query) {
          Query query = (Query) frame.message;
          CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
          if (query.options.namedValues != null
              && query.options.namedValues.size() == paramTypes.size()) {
            result.add(decodeNamedValues(paramTypes, query.options.namedValues, mapper));
          } else if (query.options.positionalValues != null
              && query.options.positionalValues.size() == paramTypes.size()) {
            result.add(decodePositionalValues(paramTypes, query.options.positionalValues, mapper));
          }
        } else if (frame.message instanceof Execute) {
          Execute execute = (Execute) frame.message;
          CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
          if (execute.options.namedValues != null
              && execute.options.namedValues.size() == paramTypes.size()) {
            result.add(decodeNamedValues(paramTypes, execute.options.namedValues, mapper));
          } else if (execute.options.positionalValues != null
              && execute.options.positionalValues.size() == paramTypes.size()) {
            result.add(
                decodePositionalValues(paramTypes, execute.options.positionalValues, mapper));
          }
        }
      }
    } else if (primedRequest.when instanceof Batch) {

      List<Statement> queries = ((Batch) primedRequest.when).queries;

      if (frame.message instanceof com.datastax.oss.protocol.internal.request.Batch) {
        com.datastax.oss.protocol.internal.request.Batch batch =
            (com.datastax.oss.protocol.internal.request.Batch) frame.message;

        Iterator<List<ByteBuffer>> valuesIterator = batch.values.iterator();
        Iterator<Statement> statementIterator = queries.iterator();

        CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
        while (valuesIterator.hasNext()) {
          List<ByteBuffer> values = valuesIterator.next();
          Statement statement = statementIterator.next();
          result.add(decodePositionalValues(statement.paramTypes, values, mapper));
        }
      }
    }
    return result;
  }

  private static LinkedHashMap<String, Object> decodePositionalValues(
      Map<String, String> paramTypes, List<ByteBuffer> positionalValues, CqlMapper mapper) {

    AtomicInteger index = new AtomicInteger();
    return paramTypes.entrySet().stream()
        .collect(
            LinkedHashMap::new,
            (map, entry) -> {
              Codec<Object> codec = codecFor(mapper, entry.getValue());
              ByteBuffer encodedValue = positionalValues.get(index.getAndIncrement());
              Object decodedValue = encodedValue == null ? null : codec.decode(encodedValue);
              map.put(entry.getKey(), decodedValue);
            },
            LinkedHashMap::putAll);
  }

  private static LinkedHashMap<String, Object> decodeNamedValues(
      Map<String, String> paramTypes, Map<String, ByteBuffer> namedValues, CqlMapper mapper) {

    return paramTypes.entrySet().stream()
        .collect(
            LinkedHashMap::new,
            (map, entry) -> {
              Codec<Object> codec = codecFor(mapper, entry.getValue());
              ByteBuffer encodedValue = namedValues.get(entry.getKey());
              Object decodedValue = encodedValue == null ? null : codec.decode(encodedValue);
              map.put(entry.getKey(), decodedValue);
            },
            LinkedHashMap::putAll);
  }

  private static Codec<Object> codecFor(CqlMapper mapper, String typeName) {
    RawType rawType = CodecUtils.getTypeFromName(typeName);
    return mapper.codecFor(rawType);
  }
}
