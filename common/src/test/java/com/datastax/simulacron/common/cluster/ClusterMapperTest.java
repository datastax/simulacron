package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterMapperTest {

  ObjectMapper mapper = ClusterMapper.getMapper();

  @Test
  public void testSimpleCluster() throws Exception {
    Cluster cluster =
        Cluster.builder().withName("cluster1").withDSEVersion("5.1.0").withNodes(1, 2).build();

    String json = mapper.writeValueAsString(cluster);

    String expectedJson =
        "{\"name\":\"cluster1\",\"dse_version\":\"5.1.0\",\"data_centers\":["
            + "{\"name\":\"dc1\",\"id\":0,\"nodes\":[{\"name\":\"node1\",\"id\":0}]},"
            + "{\"name\":\"dc2\",\"id\":1,\"nodes\":[{\"name\":\"node1\",\"id\":0},{\"name\":\"node2\",\"id\":1}"
            + "]}]}";
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
            + "{\"name\":\"0\",\"id\":0,\"address\":\"127.0.0.1:9042\"},"
            + "{\"name\":\"1\",\"id\":1,\"address\":\"127.0.0.2:9042\"}"
            + "]}]}";
    assertThat(json).isEqualTo(expectedJson);

    Cluster cluster2 = mapper.readValue(json, Cluster.class);

    // Ensure the addresses get created on deserialization
    assertThat(cluster2.getNodes().get(0).getAddress()).isEqualTo(addr1);
    assertThat(cluster2.getNodes().get(1).getAddress()).isEqualTo(addr2);
  }
}
