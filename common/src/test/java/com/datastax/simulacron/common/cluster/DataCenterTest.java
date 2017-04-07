package com.datastax.simulacron.common.cluster;

import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class DataCenterTest {

  @Test
  public void testShouldSetClusterToParent() {
    Cluster cluster = Cluster.builder().build();
    DataCenter dataCenter = cluster.addDataCenter().build();
    assertThat(dataCenter.getCluster()).isSameAs(cluster);
    assertThat(dataCenter.getParent()).isEqualTo(Optional.of(cluster));
  }

  @Test
  public void testShouldInheritBuilderProperties() {
    long id = 12L;
    String version = "1.2.19";
    String name = "node0";
    Map<String, Object> peerInfo = new HashMap<>();
    peerInfo.put("hello", "world");
    Cluster cluster = Cluster.builder().build();
    DataCenter dc =
        cluster
            .addDataCenter()
            .withId(id)
            .withCassandraVersion(version)
            .withName(name)
            .withPeerInfo(peerInfo)
            .withPeerInfo("goodbye", "world")
            .build();

    Map<String, Object> expectedPeerInfo = new HashMap<>(peerInfo);
    expectedPeerInfo.put("goodbye", "world");

    assertThat(dc.getId()).isEqualTo(id);
    assertThat(dc.getCassandraVersion()).isEqualTo(version);
    assertThat(dc.getName()).isEqualTo(name);
    assertThat(dc.getPeerInfo()).isEqualTo(expectedPeerInfo);
    assertThat(dc.getCluster()).isSameAs(cluster);
    assertThat(dc.getParent()).isEqualTo(Optional.of(cluster));
  }

  @Test
  public void testAddNodeShouldAssignId() {
    DataCenter dc = new DataCenter();
    for (int i = 0; i < 5; i++) {
      Node node = dc.addNode().build();
      assertThat(node.getId()).isEqualTo((long) i);
      assertThat(node.getDataCenter()).isEqualTo(dc);
      assertThat(node.getParent()).isEqualTo(Optional.of(dc));
      assertThat(dc.getNodes()).contains(node);
    }
    assertThat(dc.getNodes()).hasSize(5);
  }

  @Test
  public void testDefaultConstructor() {
    // This is only used by jackson mapper, but ensure it has sane defaults and doesn't throw any exceptions.
    DataCenter dc = new DataCenter();
    assertThat(dc.getCluster()).isNull();
    assertThat(dc.getParent()).isEmpty();
    assertThat(dc.getId()).isEqualTo(null);
    assertThat(dc.getCassandraVersion()).isEqualTo(null);
    assertThat(dc.getName()).isEqualTo(null);
    assertThat(dc.getPeerInfo()).isEqualTo(Collections.emptyMap());
    assertThat(dc.getNodes()).isEmpty();
    assertThat(dc.toString()).isNotNull();
  }
}
