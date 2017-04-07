package com.datastax.simulacron.common.cluster;

import org.junit.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterTest {

  @Test
  public void testShouldNotHaveParent() {
    Cluster cluster = Cluster.builder().build();
    assertThat(cluster.getParent()).isEmpty();
  }

  @Test
  public void testShouldInheritBuilderProperties() {
    long id = 12L;
    String version = "1.2.19";
    String name = "node0";
    Map<String, Object> peerInfo = new HashMap<>();
    peerInfo.put("hello", "world");
    Cluster cluster =
        Cluster.builder()
            .withId(id)
            .withCassandraVersion(version)
            .withName(name)
            .withPeerInfo(peerInfo)
            .withPeerInfo("goodbye", "world")
            .build();

    Map<String, Object> expectedPeerInfo = new HashMap<>(peerInfo);
    expectedPeerInfo.put("goodbye", "world");

    assertThat(cluster.getId()).isEqualTo(id);
    assertThat(cluster.getCassandraVersion()).isEqualTo(version);
    assertThat(cluster.getName()).isEqualTo(name);
    assertThat(cluster.getPeerInfo()).isEqualTo(expectedPeerInfo);
  }

  @Test
  public void testAddDataCenterShouldAssignId() {
    Cluster cluster = Cluster.builder().build();
    for (int i = 0; i < 5; i++) {
      DataCenter dc = cluster.addDataCenter().build();
      assertThat(dc.getId()).isEqualTo((long) i);
      assertThat(dc.getCluster()).isEqualTo(cluster);
      assertThat(dc.getParent()).isEqualTo(Optional.of(cluster));
      assertThat(cluster.getDataCenters()).contains(dc);
      for (int j = 0; j < 5; j++) {
        Node node = dc.addNode().build();
        assertThat(node.getId()).isEqualTo((long) j);
        assertThat(node.getDataCenter()).isEqualTo(dc);
        assertThat(node.getParent()).isEqualTo(Optional.of(dc));
        assertThat(node.getCluster()).isEqualTo(cluster);
        assertThat(cluster.getNodes()).contains(node);
      }
    }
    assertThat(cluster.getDataCenters()).hasSize(5);
    assertThat(cluster.getNodes()).hasSize(25);
  }

  @Test
  public void testDefaultConstructor() {
    // This is only used by jackson mapper, but ensure it has sane defaults and doesn't throw any exceptions.
    Cluster cluster = new Cluster();
    assertThat(cluster.getParent()).isEmpty();
    assertThat(cluster.getId()).isEqualTo(null);
    assertThat(cluster.getCassandraVersion()).isEqualTo(null);
    assertThat(cluster.getName()).isEqualTo(null);
    assertThat(cluster.getPeerInfo()).isEqualTo(Collections.emptyMap());
    assertThat(cluster.getDataCenters()).isEmpty();
    assertThat(cluster.getNodes()).isEmpty();
    assertThat(cluster.toString()).isNotNull();
  }

  @Test
  public void testBuilderWithNodes() {
    Cluster cluster = Cluster.builder().withNodes(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).build();

    Collection<DataCenter> dcs = cluster.getDataCenters();
    assertThat(dcs).hasSize(11);
    Iterator<DataCenter> it = dcs.iterator();
    for (int i = 0; i < 11; i++) {
      DataCenter dc = it.next();
      assertThat(dc.getNodes()).hasSize(i);
      assertThat(dc.getId()).isEqualTo(i);
    }
  }

  @Test
  public void testCopy() {
    Cluster cluster =
        Cluster.builder()
            .withId(1L)
            .withName("cluster0")
            .withCassandraVersion("3.0.11")
            .withPeerInfo("hello", "world")
            .build();

    cluster.addDataCenter().build();
    cluster.addDataCenter().build();

    Cluster cluster2 = Cluster.builder().copy(cluster).build();
    assertThat(cluster2.getId()).isEqualTo(cluster.getId());
    assertThat(cluster2.getName()).isEqualTo(cluster.getName());
    assertThat(cluster2.getCassandraVersion()).isEqualTo(cluster.getCassandraVersion());
    assertThat(cluster2.getPeerInfo()).isEqualTo(cluster.getPeerInfo());

    // DataCenters should not be copied.
    assertThat(cluster2.getDataCenters()).isEmpty();
  }
}
