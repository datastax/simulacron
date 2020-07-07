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
package com.datastax.oss.simulacron.http.server;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.ObjectMapperHolder;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;

public class HttpPrimeBatchIntegrationTest {

  @Rule public AdminServer server = new AdminServer(ClusterSpec.builder().withNodes(1).build());

  ObjectMapper om = ObjectMapperHolder.getMapper();

  @Test
  public void testBatchPrimeSimple() throws Exception {
    String query = "INSERT INTO a.b(c, d) VALUES( (?, ?)";

    Map<String, String> param_types = new HashMap<String, String>();
    param_types.put("column1", "ascii");
    param_types.put("column2", "int");

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("column1", "column1");
    params.put("column2", "2");

    RequestPrime prime = HttpTestUtil.createSimpleParameterizedBatch(query, param_types, params);
    HttpTestResponse response = server.prime(prime);
    assertNotNull(response);
    RequestPrime responseQuery = om.readValue(response.body, RequestPrime.class);
    assertThat(responseQuery).isEqualTo(prime);

    String contactPoint = HttpTestUtil.getContactPointString(server.getCluster(), 0);
    BatchStatement statement =
        HttpTestUtil.makeNativeBatchStatement(
            Arrays.asList(query), Arrays.asList(Arrays.asList("column1", 2)));

    ResultSet set = HttpTestUtil.executeQueryWithFreshSession(statement, contactPoint);

    assertResult(set);
  }

  @Test
  public void testBatchPrimeWithPreparedPositional() throws Exception {
    String query = "INSERT INTO a.c(c1) VALUES (?)";
    String simple_query = "INSERT INTO a.b(c, d) VALUES( (?, ?)";

    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("c1", "c1");
    RequestPrime prime = HttpTestUtil.createSimpleParameterizedQuery(query, params, paramTypes);
    server.prime(prime);

    String contactPoint = HttpTestUtil.getContactPointString(server.getCluster(), 0);
    BoundStatement bt = HttpTestUtil.getBoundStatement(query, contactPoint, "c1");

    Map<String, String> simple_param_types = new HashMap<String, String>();
    simple_param_types.put("column1", "ascii");
    simple_param_types.put("column2", "int");

    Map<String, Object> simple_params = new HashMap<String, Object>();
    simple_params.put("column1", "column1");
    simple_params.put("column2", "2");

    Map<String, String> param_types = new HashMap<String, String>();
    param_types.put("c1", "ascii");

    Map<String, Object> params_batch = new HashMap<String, Object>();
    params_batch.put("c1", "c1");

    RequestPrime primeBatch =
        HttpTestUtil.createParameterizedBatch(
            Arrays.asList(simple_query, query),
            Arrays.asList(simple_param_types, param_types),
            Arrays.asList(simple_params, params_batch));

    HttpTestResponse response = server.prime(primeBatch);
    assertNotNull(response);
    RequestPrime responseQuery = om.readValue(response.body, RequestPrime.class);
    assertThat(responseQuery).isEqualTo(primeBatch);

    BatchStatement statement =
        HttpTestUtil.makeNativeBatchStatement(
            Arrays.asList(simple_query), Arrays.asList(Arrays.asList("column1", 2)));
    statement.add(bt);

    ResultSet set = HttpTestUtil.executeQueryWithFreshSession(statement, contactPoint);

    assertResult(set);
  }

  @Test
  public void testBatchPrimeWithPreparedNamed() throws Exception {
    String query = "INSERT INTO a.c(c1, c2) VALUES (:c1, :c2)";
    String simple_query = "INSERT INTO a.b(c, d) VALUES( (?, ?)";

    LinkedHashMap<String, String> paramTypes = new LinkedHashMap<>();
    paramTypes.put("c1", "ascii");
    paramTypes.put("c2", "ascii");
    LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("c1", "c1");
    params.put("c2", "c2");
    RequestPrime prime = HttpTestUtil.createSimpleParameterizedQuery(query, params, paramTypes);
    server.prime(prime);

    String contactPoint = HttpTestUtil.getContactPointString(server.getCluster(), 0);
    BoundStatement bt =
        HttpTestUtil.getBoundStatementNamed(
            query, contactPoint, ImmutableMap.<String, String>of("c1", "c1", "c2", "c2"));

    Map<String, String> simple_param_types = new HashMap<String, String>();
    simple_param_types.put("column1", "ascii");
    simple_param_types.put("column2", "int");

    Map<String, Object> simple_params = new HashMap<String, Object>();
    simple_params.put("column1", "column1");
    simple_params.put("column2", "2");

    Map<String, String> param_types = new HashMap<String, String>();
    param_types.put("c1", "ascii");

    Map<String, Object> params_batch = new HashMap<String, Object>();
    params_batch.put("c1", "c1");

    RequestPrime primeBatch =
        HttpTestUtil.createParameterizedBatch(
            Arrays.asList(simple_query, query),
            Arrays.asList(simple_param_types, param_types),
            Arrays.asList(simple_params, params_batch));

    HttpTestResponse response = server.prime(primeBatch);
    assertNotNull(response);
    RequestPrime responseQuery = om.readValue(response.body, RequestPrime.class);
    assertThat(responseQuery).isEqualTo(primeBatch);

    BatchStatement statement =
        HttpTestUtil.makeNativeBatchStatement(
            Arrays.asList(simple_query), Arrays.asList(Arrays.asList("column1", 2)));
    statement.add(bt);

    ResultSet set = HttpTestUtil.executeQueryWithFreshSession(statement, contactPoint);

    assertResult(set);
  }

  private void assertResult(ResultSet set) {
    List<Row> results = set.all();
    assertThat(1).isEqualTo(results.size());
    Row row1 = results.get(0);
    boolean column1 = row1.getBool("applied");
    assertThat(column1).isTrue();
  }
}
