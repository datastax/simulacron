package com.datastax.simulacron.common.cluster;

import org.junit.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class NodePropertiesTest {

  @Test
  public void testResolveDirect() {
    // should resolve values when they are set directly on their subjects.
    Cluster cluster =
        Cluster.builder()
            .withName("cluster0")
            .withId(1L)
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .build();

    DataCenter dc =
        cluster
            .addDataCenter()
            .withName("dc0")
            .withCassandraVersion("2.0.17")
            .withPeerInfo("goodbye", "sun")
            .build();

    Node node =
        dc.addNode()
            .withName("node0")
            .withId(7L)
            .withCassandraVersion("3.0.11")
            .withPeerInfo("hola", "mundo")
            .build();

    assertThat(node.resolveCassandraVersion()).isEqualTo("3.0.11");
    assertThat(node.resolveId()).isEqualTo("1:0:7");
    assertThat(node.resolveName()).isEqualTo("cluster0:dc0:node0");
    assertThat(node.resolvePeerInfo("hola")).isEqualTo(Optional.of("mundo"));
    assertThat(node.resolvePeerInfo("hola", "no")).isEqualTo("mundo");
    assertThat(node.resolvePeerInfo("yo", "hi")).isEqualTo("hi");
  }

  @Test
  public void testResolveIndirect() {
    // should resolve values when they are set directly on their subjects.
    Cluster cluster =
        Cluster.builder()
            .withName("cluster0")
            .withId(1L)
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .build();

    DataCenter dc = cluster.addDataCenter().withName("dc0").withPeerInfo("goodbye", "sun").build();

    Node node = dc.addNode().withPeerInfo("hola", "mundo").build();

    assertThat(node.resolveCassandraVersion()).isEqualTo("1.2.19");
    // Node id/name should be resolved to 0 since first node in dc.
    assertThat(node.resolveId()).isEqualTo("1:0:0");
    assertThat(node.resolveName()).isEqualTo("cluster0:dc0:0");
    assertThat(node.resolvePeerInfo("hello")).isEqualTo(Optional.of("world"));
    assertThat(node.resolvePeerInfo("goodbye")).isEqualTo(Optional.of("sun"));
    assertThat(node.resolvePeerInfo("yo", "hi")).isEqualTo("hi");
  }
}
