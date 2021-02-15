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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class StatementTest {

  private static final int PROTOCOL_VERSION = 4;
  private static final String QUERY = "INSERT INTO a.c(c1, c2) VALUES (?, ?)";

  private Object queryOrId;

  public StatementTest(Object queryOrId) {
    this.queryOrId = queryOrId;
  }

  @Parameterized.Parameters
  public static Collection queryOrId() {
    return Arrays.asList(QUERY, ByteBuffer.allocate(4).putInt(QUERY.hashCode()).array());
  }

  @Test
  public void shouldMatchQuery() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");

    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("c1", "c1");
    params.put("c2", "c2");

    List<ByteBuffer> positionalValues = new ArrayList<>(2);
    positionalValues.add(ByteBuffer.wrap("c1".getBytes(StandardCharsets.UTF_8)));
    positionalValues.add(ByteBuffer.wrap("c2".getBytes(StandardCharsets.UTF_8)));

    Statement statement = new Statement(QUERY, paramTypes, params);
    boolean result = statement.checkStatementMatch(PROTOCOL_VERSION, queryOrId, positionalValues);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldMatchQueryWhenParamValuesAreNotSet() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");

    List<ByteBuffer> positionalValues = new ArrayList<>(2);
    positionalValues.add(ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));
    positionalValues.add(ByteBuffer.wrap("any".getBytes(StandardCharsets.UTF_8)));

    Statement statement = new Statement(QUERY, paramTypes, null);
    boolean result = statement.checkStatementMatch(PROTOCOL_VERSION, queryOrId, positionalValues);

    assertThat(result).isTrue();
  }

  @Test
  public void shouldNotMatchQueryEvenWhenParamsAreNotSet() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");

    List<ByteBuffer> positionalValues = new ArrayList<>(2);
    positionalValues.add(ByteBuffer.wrap("c1".getBytes(StandardCharsets.UTF_8)));
    positionalValues.add(ByteBuffer.wrap("c2".getBytes(StandardCharsets.UTF_8)));

    Statement statement = new Statement("Not matching query", paramTypes, null);
    boolean result = statement.checkStatementMatch(PROTOCOL_VERSION, queryOrId, positionalValues);

    assertThat(result).isFalse();
  }

  @Test
  public void shouldNotMatchWhenWrongNumberOfArguments() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");

    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("c1", "c1");
    params.put("c2", "c2");

    List<ByteBuffer> positionalValues = new ArrayList<>(2);
    positionalValues.add(ByteBuffer.wrap("c1".getBytes(StandardCharsets.UTF_8)));

    Statement statement = new Statement(QUERY, paramTypes, params);
    boolean result = statement.checkStatementMatch(PROTOCOL_VERSION, queryOrId, positionalValues);

    assertThat(result).isFalse();
  }

  @Test
  public void shouldNotMatchWhenWrongOrderOfArguments() {
    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");

    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("c1", "c1");
    params.put("c2", "c2");

    List<ByteBuffer> positionalValues = new ArrayList<>(2);
    positionalValues.add(ByteBuffer.wrap("c2".getBytes(StandardCharsets.UTF_8)));
    positionalValues.add(ByteBuffer.wrap("c1".getBytes(StandardCharsets.UTF_8)));

    Statement statement = new Statement(QUERY, paramTypes, params);
    boolean result = statement.checkStatementMatch(PROTOCOL_VERSION, queryOrId, positionalValues);

    assertThat(result).isFalse();
  }
}
