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
package com.datastax.oss.simulacron.common.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.simulacron.common.request.Options;
import com.datastax.oss.simulacron.common.request.Query;
import com.datastax.oss.simulacron.common.result.NoResult;
import com.datastax.oss.simulacron.common.result.Result;
import com.datastax.oss.simulacron.common.result.SuccessResult;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ObjectMapperHolderTest {

  private ObjectMapper mapper = ObjectMapperHolder.getMapper();

  @Test
  public void testSimpleCluster() throws Exception {
    ClusterSpec cluster =
        ClusterSpec.builder().withName("cluster1").withDSEVersion("5.1.0").withNodes(1, 2).build();

    String json = mapper.writeValueAsString(cluster);

    String expectedJson =
        String.format(
            "{\"name\":\"cluster1\",\"dse_version\":\"5.1.0\",\"data_centers\":["
                + "{\"name\":\"dc1\",\"id\":0,\"nodes\":[{\"name\":\"node1\",\"id\":0,\"host_id\":\"%s\",\"active_connections\":0}],\"active_connections\":0},"
                + "{\"name\":\"dc2\",\"id\":1,\"nodes\":[{\"name\":\"node1\",\"id\":0,\"host_id\":\"%s\",\"active_connections\":0},{\"name\":\"node2\",\"id\":1,\"host_id\":\"%s\",\"active_connections\":0}],\"active_connections\":0}"
                + "],\"active_connections\":0}",
            cluster.node(0, 0).getHostId(),
            cluster.node(1, 0).getHostId(),
            cluster.node(1, 1).getHostId());
    assertThat(json).isEqualTo(expectedJson);

    ClusterSpec cluster2 = mapper.readValue(json, ClusterSpec.class);

    assertThat(cluster2.getName()).isEqualTo("cluster1");
    assertThat(cluster2.getDataCenters()).hasSize(2);
    assertThat(cluster2.getNodes()).hasSize(3);
  }

  @Test
  public void testClusterWithAddresses() throws Exception {
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc = cluster.addDataCenter().build();
    // Add two nodes with preassigned ip addresses.
    byte[] local1 = {127, 0, 0, 1};
    byte[] local2 = {127, 0, 0, 2};
    InetSocketAddress addr1 = new InetSocketAddress(InetAddress.getByAddress(local1), 9042);
    InetSocketAddress addr2 = new InetSocketAddress(InetAddress.getByAddress(local2), 9042);
    dc.addNode().withAddress(addr1).build();
    dc.addNode().withAddress(addr2).build();

    String json = mapper.writeValueAsString(cluster);

    String expectedJson =
        String.format(
            "{\"data_centers\":[{\"name\":\"0\",\"id\":0,\"nodes\":["
                + "{\"name\":\"0\",\"id\":0,\"address\":\"127.0.0.1:9042\",\"host_id\":\"%s\",\"active_connections\":0},"
                + "{\"name\":\"1\",\"id\":1,\"address\":\"127.0.0.2:9042\",\"host_id\":\"%s\",\"active_connections\":0}"
                + "],\"active_connections\":0}],\"active_connections\":0}",
            cluster.node(0, 0).getHostId(), cluster.node(0, 1).getHostId());
    assertThat(json).isEqualTo(expectedJson);

    ClusterSpec cluster2 = mapper.readValue(json, ClusterSpec.class);

    // Ensure the addresses get created on deserialization
    assertThat(cluster2.node(0).getAddress()).isEqualTo(addr1);
    assertThat(cluster2.node(1).getAddress()).isEqualTo(addr2);
  }

  @Test
  public void testPrimeQuery() throws Exception {
    Query when = new Query("SELECT * table_name");
    List<Map<String, Object>> rows = new ArrayList<>();
    HashMap<String, Object> row1 = new HashMap<>();
    row1.put("column1", "column1");
    row1.put("column2", "2");
    rows.add(row1);
    Map<String, String> column_types = new HashMap<>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");
    Result then = new SuccessResult(rows, column_types);
    RequestPrime requestPrime = new RequestPrime(when, then);

    String json = mapper.writeValueAsString(requestPrime);

    String expectedJson =
        "{\"when\":{\"request\":\"query\",\"query\":\"SELECT * table_name\"},\"then\":{\"result\":\"success\",\"rows\":[{\"column1\":\"column1\",\"column2\":\"2\"}],\"column_types\":{\"column1\":\"ascii\",\"column2\":\"bigint\"},\"delay_in_ms\":0,\"ignore_on_prepare\":true}}";

    assertThat(json).isEqualTo(expectedJson);

    RequestPrime readRequestPrime = mapper.readValue(json, RequestPrime.class);
    assertThat(readRequestPrime.when).isEqualTo(when);
    assertThat(readRequestPrime.then).isEqualTo(then);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrimeQueryWithRowNull() throws Exception {
    Query when = new Query("SELECT * table_name");

    Map<String, String> column_types = new HashMap<>();
    column_types.put("column1", "ascii");
    column_types.put("column2", "bigint");

    Result then = new SuccessResult(null, column_types);
    RequestPrime requestPrime = new RequestPrime(when, then);
    String json = mapper.writeValueAsString(requestPrime);
    mapper.readValue(json, RequestPrime.class);
  }

  @Test
  public void testPrimeQueryWithNulls() throws Exception {
    Query when = new Query("SELECT * table_name");
    Result then = new SuccessResult(null, null);

    RequestPrime requestPrime = new RequestPrime(when, then);
    String json = mapper.writeValueAsString(requestPrime);
    RequestPrime readRequestPrime = mapper.readValue(json, RequestPrime.class);
    assertThat(readRequestPrime.when).isEqualTo(when);
    assertThat(readRequestPrime.then)
        .isEqualTo(new SuccessResult(new ArrayList<>(), new HashMap<>()));
  }

  @Test
  public void testPrimeQueryWithNoThen() throws Exception {
    Query when = new Query("SELECT * table_name");

    RequestPrime requestPrime = new RequestPrime(when, null);
    String json = mapper.writeValueAsString(requestPrime);
    RequestPrime readRequestPrime = mapper.readValue(json, RequestPrime.class);

    assertThat(readRequestPrime.when).isEqualTo(when);
    assertThat(readRequestPrime.then).isEqualTo(new NoResult());
  }

  @Test
  public void testPrimeOptions() throws Exception {
    Options when = Options.INSTANCE;
    Result then = new NoResult();
    then.setIgnoreOnPrepare(false);
    RequestPrime requestPrime = new RequestPrime(when, then);

    String json = mapper.writeValueAsString(requestPrime);

    String expectedJson =
        "{\"when\":{\"request\":\"options\"},\"then\":{\"result\":\"no_result\",\"delay_in_ms\":0,\"ignore_on_prepare\":false}}";

    assertThat(json).isEqualTo(expectedJson);

    RequestPrime readRequestPrime = mapper.readValue(json, RequestPrime.class);
    assertThat(readRequestPrime.when).isEqualTo(when);
    assertThat(readRequestPrime.then).isEqualTo(then);

    Frame frame =
        new Frame(
            6,
            false,
            0,
            false,
            null,
            FrameUtils.emptyCustomPayload,
            Collections.emptyList(),
            com.datastax.oss.protocol.internal.request.Options.INSTANCE);

    assertThat(when.matches(frame)).isTrue();
  }
}
