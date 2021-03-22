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
package com.datastax.oss.simulacron.common.request;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class QueryTest {

  private static final String SIMPLE_SELECT_QUERY =
      "select foo, bar from test_ks.test_tab where foo = ? and bar = ?";
  private static final String SIMPLE_SELECT_QUERY_NAMED_PARAMS =
      "select foo, bar from test_ks.test_tab where foo = :foo and bar = :bar";

  @Test
  public void shouldAlwaysMatchWhenParamValuesAreNotSet() {
    // Ensures that if a query prime is set up where there are no param values configured that it
    // matches every query matching the query text.
    String queryStr = "update x set y = ?, z = ?, a = ? where b = ?";
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("y", "int");
    paramTypes.put("z", "int");
    paramTypes.put("a", "varchar");
    paramTypes.put("b", "ascii");
    Query query0 = new Query(queryStr, new String[] {}, null, paramTypes);

    com.datastax.oss.protocol.internal.request.Query simpleQueryNoArgs =
        new com.datastax.oss.protocol.internal.request.Query(queryStr);

    assertThat(query0.matches(FrameUtils.wrapRequest(simpleQueryNoArgs))).isTrue();

    List<ByteBuffer> posValues = new ArrayList<>();
    posValues.add(ByteBuffer.wrap(new byte[] {1, 2, 3, 4}));
    QueryOptions queryOptions = getQueryOptions(posValues, Collections.emptyMap());
    com.datastax.oss.protocol.internal.request.Query simpleQueryWithArgs =
        new com.datastax.oss.protocol.internal.request.Query(queryStr, queryOptions);

    assertThat(query0.matches(FrameUtils.wrapRequest(simpleQueryWithArgs))).isTrue();

    Map<String, ByteBuffer> namedValues = new HashMap<>();
    namedValues.put("z", ByteBuffer.wrap(new byte[] {0, 0, 0, 5}));

    queryOptions = getQueryOptions(Collections.emptyList(), namedValues);
    com.datastax.oss.protocol.internal.request.Query simpleQueryWithNamedArgs =
        new com.datastax.oss.protocol.internal.request.Query(queryStr, queryOptions);

    assertThat(query0.matches(FrameUtils.wrapRequest(simpleQueryWithNamedArgs))).isTrue();

    Execute execute =
        new Execute(BigInteger.valueOf(queryStr.hashCode()).toByteArray(), queryOptions);
    assertThat(query0.matches(FrameUtils.wrapRequest(execute))).isTrue();

    // should also work when values are an empty map.
    Query query1 = new Query(queryStr, new String[] {}, new LinkedHashMap<>(), paramTypes);
    assertThat(query1.matches(FrameUtils.wrapRequest(execute))).isTrue();
  }

  @Test
  public void shouldPassForQueryMessageWithPositionalParams() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY);
    Frame frame = FrameUtils.wrapRequest(createQueryMessageWithPositionalValues());

    boolean result = selectQuery.matches(frame);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldFailForQueryMessageWithPositionalParamsWhenParamsNotMatch() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY);

    List<ByteBuffer> reverseOrderedPositionalParamValues =
        Arrays.asList(
            ByteBuffer.wrap(new byte[] {0, 0, 0, 100}),
            ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));

    QueryOptions invalidQueryOptions =
        getQueryOptions(reverseOrderedPositionalParamValues, Collections.emptyMap());

    Frame frame =
        FrameUtils.wrapRequest(
            new com.datastax.oss.protocol.internal.request.Query(
                SIMPLE_SELECT_QUERY, invalidQueryOptions));

    boolean result = selectQuery.matches(frame);

    assertThat(result).isFalse();
  }

  @Test
  public void testNamedParams() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY_NAMED_PARAMS);

    Map<String, ByteBuffer> namedParamValues = new HashMap<>();
    namedParamValues.put("foo", ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    namedParamValues.put("bar", ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));

    QueryOptions queryOptions = getQueryOptions(Collections.emptyList(), namedParamValues);

    Frame frame =
        FrameUtils.wrapRequest(
            new com.datastax.oss.protocol.internal.request.Query(
                SIMPLE_SELECT_QUERY_NAMED_PARAMS, queryOptions));

    boolean result = selectQuery.matches(frame);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldPassWhenQueryConsistencyMatchExecuteMessage() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY, "ANY");
    Frame frame = FrameUtils.wrapRequest(createExecuteMessageWithPositionalValues());

    boolean result = selectQuery.matches(frame);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldFailWhenQueryConsistencyNotMatchExecuteMessage() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY, "ONE");
    Frame frame = FrameUtils.wrapRequest(createExecuteMessageWithPositionalValues());

    boolean result = selectQuery.matches(frame);

    assertThat(result).isFalse();
  }

  @Test
  public void shouldPassWhenQueryConsistencyMatchQueryMessage() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY, "ANY");
    Frame frame = FrameUtils.wrapRequest(createQueryMessageWithPositionalValues());

    boolean result = selectQuery.matches(frame);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldFailWhenQueryConsistencyNotMatchQueryMessage() {
    Query selectQuery = createQueryUnderTest(SIMPLE_SELECT_QUERY, "ONE");
    Frame frame = FrameUtils.wrapRequest(createQueryMessageWithPositionalValues());

    boolean result = selectQuery.matches(frame);

    assertThat(result).isFalse();
  }

  private Query createQueryUnderTest(String queryStr, String... consistencyLevels) {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("foo", "varchar");
    paramTypes.put("bar", "int");
    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("foo", "any");
    params.put("bar", 100);

    return new Query(queryStr, consistencyLevels, params, paramTypes);
  }

  private Execute createExecuteMessageWithPositionalValues() {
    List<ByteBuffer> positionalParamValues =
        Arrays.asList(
            ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));

    QueryOptions queryOptions = getQueryOptions(positionalParamValues, Collections.emptyMap());

    return new Execute(
        BigInteger.valueOf(SIMPLE_SELECT_QUERY.hashCode()).toByteArray(), queryOptions);
  }

  private com.datastax.oss.protocol.internal.request.Query
      createQueryMessageWithPositionalValues() {

    List<ByteBuffer> positionalParamValues =
        Arrays.asList(
            ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)),
            ByteBuffer.wrap(new byte[] {0, 0, 0, 100}));

    QueryOptions queryOptions = getQueryOptions(positionalParamValues, Collections.emptyMap());
    return new com.datastax.oss.protocol.internal.request.Query(SIMPLE_SELECT_QUERY, queryOptions);
  }

  private QueryOptions getQueryOptions(
      List<ByteBuffer> positionalParamValues, Map<String, ByteBuffer> objectObjectMap) {

    return new QueryOptions(
        0, positionalParamValues, objectObjectMap, true, 0, null, 10, -1, null, Integer.MIN_VALUE);
  }
}
