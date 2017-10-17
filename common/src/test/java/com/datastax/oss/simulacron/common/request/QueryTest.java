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
package com.datastax.oss.simulacron.common.request;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class QueryTest {

  @Test
  public void shouldAlwaysMatchWhenParamValuesAreNotSet() {
    // Ensures that if a query prime is set up where there are no param values configured that it matches every
    // query matching the query text.
    String queryStr = "update x set y = ?, z = ?, a = ? where b = ?";
    Map<String, String> paramTypes = new HashMap<>();
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
    QueryOptions queryOptions =
        new QueryOptions(0, posValues, Collections.emptyMap(), true, 0, null, 10, -1, null);
    com.datastax.oss.protocol.internal.request.Query simpleQueryWithArgs =
        new com.datastax.oss.protocol.internal.request.Query(queryStr, queryOptions);

    assertThat(query0.matches(FrameUtils.wrapRequest(simpleQueryWithArgs))).isTrue();

    Map<String, ByteBuffer> namedValues = new HashMap<>();
    namedValues.put("z", ByteBuffer.wrap(new byte[] {0, 0, 0, 5}));

    queryOptions =
        new QueryOptions(0, Collections.emptyList(), namedValues, true, 0, null, 10, -1, null);
    com.datastax.oss.protocol.internal.request.Query simpleQueryWithNamedArgs =
        new com.datastax.oss.protocol.internal.request.Query(queryStr, queryOptions);

    assertThat(query0.matches(FrameUtils.wrapRequest(simpleQueryWithNamedArgs))).isTrue();

    Execute execute =
        new Execute(BigInteger.valueOf(queryStr.hashCode()).toByteArray(), queryOptions);
    assertThat(query0.matches(FrameUtils.wrapRequest(execute))).isTrue();

    // should also work when values are an empty map.
    Query query1 = new Query(queryStr, new String[] {}, Collections.emptyMap(), paramTypes);
    assertThat(query1.matches(FrameUtils.wrapRequest(execute))).isTrue();
  }
}
