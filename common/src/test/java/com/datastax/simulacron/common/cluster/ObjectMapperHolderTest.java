package com.datastax.simulacron.common.cluster;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.simulacron.common.request.Options;
import com.datastax.simulacron.common.request.Query;
import com.datastax.simulacron.common.result.NoResult;
import com.datastax.simulacron.common.result.Result;
import com.datastax.simulacron.common.result.SuccessResult;
import com.datastax.simulacron.common.utils.FrameUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ObjectMapperHolderTest {

  private ObjectMapper mapper = ObjectMapperHolder.getMapper();

  @Test
  public void testSimpleCluster() throws Exception {
    Cluster cluster =
        Cluster.builder().withName("cluster1").withDSEVersion("5.1.0").withNodes(1, 2).build();

    String json = mapper.writeValueAsString(cluster);

    String expectedJson =
        "{\"name\":\"cluster1\",\"dse_version\":\"5.1.0\",\"data_centers\":["
            + "{\"name\":\"dc1\",\"id\":0,\"nodes\":[{\"name\":\"node1\",\"id\":0,\"active_connections\":0}],\"active_connections\":0},"
            + "{\"name\":\"dc2\",\"id\":1,\"nodes\":[{\"name\":\"node1\",\"id\":0,\"active_connections\":0},{\"name\":\"node2\",\"id\":1,\"active_connections\":0}],\"active_connections\":0}"
            + "],\"active_connections\":0}";
    assertThat(json).isEqualTo(expectedJson);

    Cluster cluster2 = mapper.readValue(json, Cluster.class);

    assertThat(cluster2.getName()).isEqualTo("cluster1");
    assertThat(cluster2.getDataCenters()).hasSize(2);
    assertThat(cluster2.getNodes()).hasSize(3);
  }

  @Test
  public void testClusterWithAddresses() throws Exception {
    Cluster cluster = Cluster.builder().build();
    DataCenter dc = cluster.addDataCenter().build();
    // Add two nodes with preassigned ip addresses.
    byte[] local1 = {127, 0, 0, 1};
    byte[] local2 = {127, 0, 0, 2};
    InetSocketAddress addr1 = new InetSocketAddress(InetAddress.getByAddress(local1), 9042);
    InetSocketAddress addr2 = new InetSocketAddress(InetAddress.getByAddress(local2), 9042);
    dc.addNode().withAddress(addr1).build();
    dc.addNode().withAddress(addr2).build();

    String json = mapper.writeValueAsString(cluster);

    String expectedJson =
        "{\"data_centers\":[{\"name\":\"0\",\"id\":0,\"nodes\":["
            + "{\"name\":\"0\",\"id\":0,\"address\":\"127.0.0.1:9042\",\"active_connections\":0},"
            + "{\"name\":\"1\",\"id\":1,\"address\":\"127.0.0.2:9042\",\"active_connections\":0}"
            + "],\"active_connections\":0}],\"active_connections\":0}";
    assertThat(json).isEqualTo(expectedJson);

    Cluster cluster2 = mapper.readValue(json, Cluster.class);

    // Ensure the addresses get created on deserialization
    assertThat(cluster2.getNodes().get(0).getAddress()).isEqualTo(addr1);
    assertThat(cluster2.getNodes().get(1).getAddress()).isEqualTo(addr2);
  }

  @Test
  public void testPrimeQuery() throws Exception {
    Query when = new Query("SELECT * table_name", null, null, null);
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
        "{\"when\":{\"request\":\"query\",\"query\":\"SELECT * table_name\"},\"then\":{\"result\":\"success\",\"rows\":[{\"column1\":\"column1\",\"column2\":\"2\"}],\"column_types\":{\"column1\":\"ascii\",\"column2\":\"bigint\"},\"delay_in_ms\":0}}";

    assertThat(json).isEqualTo(expectedJson);

    RequestPrime readRequestPrime = mapper.readValue(json, RequestPrime.class);
    assertThat(readRequestPrime.when).isEqualTo(when);
    assertThat(readRequestPrime.then).isEqualTo(then);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testPrimeQueryWithRowNull() throws Exception {
    Query when = new Query("SELECT * table_name", null, null, null);

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
    Query when = new Query("SELECT * table_name", null, null, null);
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
    Query when = new Query("SELECT * table_name", null, null, null);

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
    RequestPrime requestPrime = new RequestPrime(when, then);

    String json = mapper.writeValueAsString(requestPrime);

    String expectedJson =
        "{\"when\":{\"request\":\"options\"},\"then\":{\"result\":\"no_result\",\"delay_in_ms\":0}}";

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
