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
package com.datastax.oss.simulacron.http.server;

import static com.datastax.oss.simulacron.driver.SimulacronDriverSupport.defaultBuilder;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.codec.WriteType;
import com.datastax.oss.simulacron.common.request.Batch;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.result.WriteTimeoutResult;
import com.datastax.oss.simulacron.server.BoundCluster;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class HttpTestUtil {

  public static ResultSet makeNativeQuery(String query, String contactPoint) {

    SimpleStatement statement = new SimpleStatement(query);
    return executeQueryWithFreshSession(statement, contactPoint);
  }

  public static ResultSet makeNativeQueryWithNameParams(
      String query, String contactPoint, Map<String, Object> values) {
    SimpleStatement statement = new SimpleStatement(query, values);
    return executeQueryWithFreshSession(statement, contactPoint);
  }

  public static ResultSet makeNativeBoundQueryWithNameParams(
      String query, String contactPoint, Map<String, Long> values) {
    com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build();
    Session session = cluster.connect();
    com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
    BoundStatement bound = prepared.bind();
    values.forEach((k, v) -> bound.setLong(k, v));
    return executeQueryWithFreshSession(bound, contactPoint, session, cluster);
  }

  public static ResultSet makeNativeBoundQueryWithNameParamsStrings(
      String query, String contactPoint, Map<String, String> values) {
    com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build();
    Session session = cluster.connect();
    com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
    BoundStatement bound = getBoundStatementNamed(query, contactPoint, values);
    return executeQueryWithFreshSession(bound, contactPoint, session, cluster);
  }

  public static ResultSet makeNativeQueryWithPositionalParams(
      String query, String contactPoint, Object... values) {
    SimpleStatement statement = new SimpleStatement(query, values);
    return executeQueryWithFreshSession(statement, contactPoint);
  }

  public static ResultSet makeNativeBoundQueryWithPositionalParam(
      String query, String contactPoint, Object param) {
    com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build();
    Session session = cluster.connect();
    com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
    BoundStatement bound = getBoundStatement(query, contactPoint, param);
    return executeQueryWithFreshSession(bound, contactPoint, session, cluster);
  }

  public static void makeNativeBoundQueryWithPositionalParamExpectingError(
      String query, String contactPoint, Object param, boolean errorOnPrepare) throws Exception {
    try (com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build()) {
      Session session = cluster.connect();

      if (errorOnPrepare) {
        try {
          com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
          throw new Exception("Prepared statement should have thrown exception");
        } catch (QueryExecutionException e) {
          // An exception should be throw for the prepared statement
        }
      } else {
        com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
        BoundStatement bound = getBoundStatement(query, contactPoint, param);
        try {
          executeQueryWithFreshSession(bound, contactPoint, session, cluster);
          throw new Exception("Bound statement should have thrown exception");
        } catch (QueryExecutionException e) {
          // An exception should be throw for the bound statement, not the prepared one
        }
      }
    }
  }

  public static BoundStatement getBoundStatement(String query, String contactPoint, Object param) {
    try (com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build()) {
      Session session = cluster.connect();
      com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
      BoundStatement bound = prepared.bind(param);
      return bound;
    }
  }

  public static BoundStatement getBoundStatementNamed(
      String query, String contactPoint, Map<String, String> values) {
    try (com.datastax.driver.core.Cluster cluster =
        defaultBuilder().addContactPoint(contactPoint).build()) {
      Session session = cluster.connect();
      com.datastax.driver.core.PreparedStatement prepared = session.prepare(query);
      BoundStatement bound = prepared.bind();
      values.forEach((k, v) -> bound.setString(k, v));
      return bound;
    }
  }

  public static BatchStatement makeNativeBatchStatement(List<String> queries, List<List> values) {

    BatchStatement statement = new BatchStatement();
    Iterator<List> valuesIterator = values.iterator();
    for (String query : queries) {
      List value = valuesIterator.next();
      statement.add(new SimpleStatement(query, value.toArray(new Object[value.size()])));
    }
    return statement;
  }

  public static RequestPrime createSimplePrimedQuery(String query) {
    return createSimpleParameterizedQuery(query, null, null);
  }

  public static RequestPrime createSimpleParameterizedQuery(
      String query,
      LinkedHashMap<String, Object> params,
      LinkedHashMap<String, String> paramTypes) {
    Query when = new Query(query, Collections.emptyList(), params, paramTypes);
    List<LinkedHashMap<String, Object>> rows = new ArrayList<>();
    LinkedHashMap<String, Object> row1 = new LinkedHashMap<>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    LinkedHashMap<String, String> column_types = new LinkedHashMap<>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    Result then = new SuccessResult(rows, column_types);
    RequestPrime requestPrime = new RequestPrime(when, then);
    return requestPrime;
  }

  public static RequestPrime createPrimedErrorOnQuery(
      String query,
      LinkedHashMap<String, Object> params,
      LinkedHashMap<String, String> paramTypes,
      boolean ignoreOnPrepare) {
    Query when = new Query(query, Collections.emptyList(), params, paramTypes);
    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();
    LinkedHashMap<String, Object> row1 = new LinkedHashMap<>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    Map<String, String> column_types = new HashMap<String, String>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    Result then =
        new WriteTimeoutResult(ConsistencyLevel.ALL, 2, 1, WriteType.SIMPLE, 0, ignoreOnPrepare);
    RequestPrime requestPrime = new RequestPrime(when, then);
    return requestPrime;
  }

  public static RequestPrime createParameterizedBatch(
      List<String> queries,
      List<Map<String, String>> paramTypes,
      List<Map<String, Object>> params) {
    Iterator<String> queryIterator = queries.iterator();
    Iterator<Map<String, String>> paramTypesIterator = paramTypes.iterator();
    Iterator<Map<String, Object>> paramsIterator = params.iterator();

    List<com.datastax.oss.simulacron.common.request.Statement> statements = new ArrayList<>();

    while (queryIterator.hasNext()) {
      statements.add(
          new com.datastax.oss.simulacron.common.request.Statement(
              queryIterator.next(), paramTypesIterator.next(), paramsIterator.next()));
    }

    Batch when = new Batch(statements, Collections.emptyList());

    List<LinkedHashMap<String, Object>> rows = new ArrayList<LinkedHashMap<String, Object>>();
    LinkedHashMap<String, Object> row1 = new LinkedHashMap<>();
    row1.put("applied", "true");
    rows.add(row1);

    LinkedHashMap<String, String> column_types_result = new LinkedHashMap<>();
    column_types_result.put("applied", "boolean");
    Result then = new SuccessResult(rows, column_types_result);
    return new RequestPrime(when, then);
  }

  public static RequestPrime createSimpleParameterizedBatch(
      String query, Map<String, String> paramTypes, Map<String, Object> params) {
    return createParameterizedBatch(
        Arrays.asList(query), Arrays.asList(paramTypes), Arrays.asList(params));
  }

  public static String getContactPointStringByNodeID(NodeSpec node) {
    String rawaddress = node.getAddress().toString();
    return rawaddress.substring(1, rawaddress.length() - 5);
  }

  public static String getContactPointString(BoundCluster cluster, int node) {
    String rawaddress = cluster.node(node).getAddress().toString();
    return rawaddress.substring(1, rawaddress.length() - 5);
  }

  public static String getContactPointString(BoundCluster cluster) {
    return getContactPointString(cluster, 0);
  }

  public static ResultSet executeQueryWithFreshSession(Statement statement, String contactPoint) {
    return executeQueryWithFreshSession(statement, contactPoint, null, null);
  }

  private static ResultSet executeQueryWithFreshSession(
      Statement statement,
      String contactPoint,
      Session session,
      com.datastax.driver.core.Cluster cluster) {
    if (session == null) {
      cluster = defaultBuilder().addContactPoint(contactPoint).build();
      session = cluster.connect();
    }
    ResultSet rs = session.execute(statement);
    cluster.close();
    ;
    return rs;
  }
}
