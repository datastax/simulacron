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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.simulacron.common.request.Batch;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.request.Statement;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class MessageValuesDecoderTest {

  private static final String CQL_QUERY = "update x set y = ?, z = ?, a = ? where b = ?";

  @Test
  public void testQueryMessageWithNamedValues() {
    Prime prime = createQueryPrime();

    Map<String, ByteBuffer> namedParamValues = new HashMap<>();
    namedParamValues.put("a", ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("b", ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("y", ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));
    namedParamValues.put("z", ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));

    QueryOptions queryOptions = getQueryOptions(Collections.emptyList(), namedParamValues);
    com.datastax.oss.protocol.internal.request.Query message =
        new com.datastax.oss.protocol.internal.request.Query(CQL_QUERY, queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertQueryTests(result, 1, 0);
  }

  @Test
  public void testQueryMessageWithPositionalValues() {
    Prime prime = createQueryPrime();

    ArrayList<ByteBuffer> positionalParamValues = new ArrayList<>(4);
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));
    positionalParamValues.add(ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    positionalParamValues.add(ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));

    QueryOptions queryOptions = getQueryOptions(positionalParamValues, Collections.emptyMap());
    com.datastax.oss.protocol.internal.request.Query message =
        new com.datastax.oss.protocol.internal.request.Query(CQL_QUERY, queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertQueryTests(result, 1, 0);
  }

  @Test
  public void testExecuteMessageWithNamedValues() {
    Prime prime = createQueryPrime();

    Map<String, ByteBuffer> namedParamValues = new HashMap<>();
    namedParamValues.put("a", ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("b", ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("y", ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));
    namedParamValues.put("z", ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));

    QueryOptions queryOptions = getQueryOptions(Collections.emptyList(), namedParamValues);
    Execute message =
        new Execute(BigInteger.valueOf(CQL_QUERY.hashCode()).toByteArray(), queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertQueryTests(result, 1, 0);
  }

  @Test
  public void testExecuteMessageWithPositionalValues() {
    Prime prime = createQueryPrime();

    ArrayList<ByteBuffer> positionalParamValues = new ArrayList<>(4);
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));
    positionalParamValues.add(ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    positionalParamValues.add(ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));

    QueryOptions queryOptions = getQueryOptions(positionalParamValues, Collections.emptyMap());
    Execute message =
        new Execute(BigInteger.valueOf(CQL_QUERY.hashCode()).toByteArray(), queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertQueryTests(result, 1, 0);
  }

  @Test
  public void testBatchPositionalValues() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("y", "int");
    paramTypes.put("z", "int");
    paramTypes.put("a", "varchar");
    paramTypes.put("b", "ascii");

    List<Statement> statements = new ArrayList<>(2);
    statements.add(new Statement(CQL_QUERY, paramTypes, Collections.emptyMap()));
    statements.add(new Statement(CQL_QUERY, paramTypes, Collections.emptyMap()));
    Batch batch = new Batch(statements, Collections.emptyList());
    Prime prime = new Prime(new RequestPrime(batch, null));

    List<Object> queriesOrIds = new ArrayList<>(2);
    queriesOrIds.add(BigInteger.valueOf(CQL_QUERY.hashCode()).toByteArray());
    queriesOrIds.add(BigInteger.valueOf(CQL_QUERY.hashCode()).toByteArray());

    List<List<ByteBuffer>> values = new ArrayList<>(2);
    List<ByteBuffer> positionalParamValues = new ArrayList<>(4);
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));
    positionalParamValues.add(ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    positionalParamValues.add(ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));
    values.add(positionalParamValues);
    values.add(positionalParamValues);
    com.datastax.oss.protocol.internal.request.Batch message =
        createBatchMessage(queriesOrIds, values);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertQueryTests(result, 2, 0);
    assertQueryTests(result, 2, 1);
  }

  @Test
  public void testQueryMessageWithNullNamedValues() {
    Prime prime = createQueryPrime();

    Map<String, ByteBuffer> namedParamValues = new HashMap<>();
    namedParamValues.put("a", ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("b", null);
    namedParamValues.put("y", null);
    namedParamValues.put("z", ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));

    QueryOptions queryOptions = getQueryOptions(Collections.emptyList(), namedParamValues);
    com.datastax.oss.protocol.internal.request.Query message =
        new com.datastax.oss.protocol.internal.request.Query(CQL_QUERY, queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).get("b")).isNull();
    assertThat(result.get(0).get("y")).isNull();
  }

  @Test
  public void testQueryMessageWithNullPositionalValues() {
    Prime prime = createQueryPrime();

    ArrayList<ByteBuffer> positionalParamValues = new ArrayList<>(4);
    positionalParamValues.add(null);
    positionalParamValues.add(ByteBuffer.wrap(new byte[] {0, 0, 0, 50}));
    positionalParamValues.add(null);
    positionalParamValues.add(ByteBuffer.wrap("something".getBytes(StandardCharsets.UTF_8)));

    QueryOptions queryOptions = getQueryOptions(positionalParamValues, Collections.emptyMap());
    com.datastax.oss.protocol.internal.request.Query message =
        new com.datastax.oss.protocol.internal.request.Query(CQL_QUERY, queryOptions);
    Frame frame = FrameUtils.wrapRequest(message);

    List<LinkedHashMap<String, Object>> result = MessageValuesDecoder.decode(prime, frame);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).get("a")).isNull();
    assertThat(result.get(0).get("y")).isNull();
  }

  private void assertQueryTests(
      List<LinkedHashMap<String, Object>> result, int size, int position) {
    assertThat(result).hasSize(size);
    assertThat(result.get(position).get("a")).isEqualTo("any");
    assertThat(result.get(position).get("b")).isEqualTo("something");
    assertThat(result.get(position).get("y")).isEqualTo(100);
    assertThat(result.get(position).get("z")).isEqualTo(50);
  }

  private Prime createQueryPrime() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("y", "int");
    paramTypes.put("z", "int");
    paramTypes.put("a", "varchar");
    paramTypes.put("b", "ascii");
    Query query = new Query(CQL_QUERY, new String[] {}, null, paramTypes);
    return new Prime(new RequestPrime(query, null));
  }

  private QueryOptions getQueryOptions(
      List<ByteBuffer> positionalParamValues, Map<String, ByteBuffer> objectObjectMap) {

    return new QueryOptions(
        0, positionalParamValues, objectObjectMap, true, 0, null, 10, -1, null, Integer.MIN_VALUE);
  }

  private com.datastax.oss.protocol.internal.request.Batch createBatchMessage(
      List<Object> queriesOrIds, List<List<ByteBuffer>> values) {
    return new com.datastax.oss.protocol.internal.request.Batch(
        (byte) 0, queriesOrIds, values, 0, 0, -1, null, Integer.MIN_VALUE);
  }
}
