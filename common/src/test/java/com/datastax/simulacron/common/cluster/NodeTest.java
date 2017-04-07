package com.datastax.simulacron.common.cluster;

import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeTest {

  @Test
  public void testShouldSetDCToParent() {
    Cluster cluster = Cluster.builder().build();
    DataCenter dataCenter = cluster.addDataCenter().build();
    Node node = dataCenter.addNode().build();
    assertThat(node.getDataCenter()).isSameAs(dataCenter);
    assertThat(node.getCluster()).isSameAs(cluster);
    assertThat(node.getParent()).isEqualTo(Optional.of(dataCenter));
  }

  @Test
  public void testStandalone() {
    Node node = Node.builder().build();
    assertThat(node.getDataCenter()).isNull();
    assertThat(node.getCluster()).isNull();
    assertThat(node.getParent()).isEmpty();
  }

  @Test
  public void testShouldInheritBuilderProperties() {
    long id = 12L;
    InetSocketAddress address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
    String version = "1.2.19";
    String name = "node0";
    Map<String, Object> peerInfo = new HashMap<>();
    peerInfo.put("hello", "world");
    Node node =
        Node.builder()
            .withId(id)
            .withAddress(address)
            .withCassandraVersion(version)
            .withName(name)
            .withPeerInfo(peerInfo)
            .withPeerInfo("goodbye", "world")
            .build();

    Map<String, Object> expectedPeerInfo = new HashMap<>(peerInfo);
    expectedPeerInfo.put("goodbye", "world");

    assertThat(node.getId()).isEqualTo(id);
    assertThat(node.getAddress()).isEqualTo(address);
    assertThat(node.getCassandraVersion()).isEqualTo(version);
    assertThat(node.getName()).isEqualTo(name);
    assertThat(node.getPeerInfo()).isEqualTo(expectedPeerInfo);
  }

  @Test
  public void testDefaultConstructor() {
    // This is only used by jackson mapper, but ensure it has sane defaults and doesn't throw any exceptions.
    Node node = new Node();
    assertThat(node.getDataCenter()).isNull();
    assertThat(node.getCluster()).isNull();
    assertThat(node.getParent()).isEmpty();
    assertThat(node.getId()).isEqualTo(null);
    assertThat(node.getAddress()).isEqualTo(null);
    assertThat(node.getCassandraVersion()).isEqualTo(null);
    assertThat(node.getName()).isEqualTo(null);
    assertThat(node.getPeerInfo()).isEqualTo(Collections.emptyMap());
    assertThat(node.toString()).isNotNull();
  }

  @Test
  public void testCopy() {
    Cluster cluster = Cluster.builder().build();
    DataCenter dc = cluster.addDataCenter().build();

    Node node =
        dc.addNode()
            .withName("node2")
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .withAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042))
            .build();

    Node node2 = dc.addNode().copy(node).build();
    assertThat(node2.getId()).isEqualTo(node.getId());
    assertThat(node2.getName()).isEqualTo(node.getName());
    assertThat(node2.getCassandraVersion()).isEqualTo(node.getCassandraVersion());
    assertThat(node2.getPeerInfo()).isEqualTo(node.getPeerInfo());

    // Address should not be copied.
    assertThat(node2.getAddress()).isNull();
  }
}
