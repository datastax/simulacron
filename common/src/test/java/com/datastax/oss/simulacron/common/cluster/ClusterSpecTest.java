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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class ClusterSpecTest {

  @Test
  public void testShouldNotHaveParent() {
    ClusterSpec cluster = ClusterSpec.builder().build();
    assertThat(cluster.getParent()).isEmpty();
  }

  @Test
  public void testShouldInheritBuilderProperties() {
    long id = 12L;
    String version = "1.2.19";
    String name = "node0";
    Map<String, Object> peerInfo = new HashMap<>();
    peerInfo.put("hello", "world");
    ClusterSpec cluster =
        ClusterSpec.builder()
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
    ClusterSpec cluster = ClusterSpec.builder().build();
    for (int i = 0; i < 5; i++) {
      DataCenterSpec dc = cluster.addDataCenter().build();
      assertThat(dc.getId()).isEqualTo((long) i);
      assertThat(dc.getCluster()).isEqualTo(cluster);
      assertThat(dc.getParent()).isEqualTo(Optional.of(cluster));
      assertThat(cluster.getDataCenters()).contains(dc);
      for (int j = 0; j < 5; j++) {
        NodeSpec node = dc.addNode().build();
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
    // This is only used by jackson mapper, but ensure it has sane defaults and doesn't throw any
    // exceptions.
    ClusterSpec cluster = new ClusterSpec();
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
    ClusterSpec cluster = ClusterSpec.builder().withNodes(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10).build();

    Collection<DataCenterSpec> dcs = cluster.getDataCenters();
    assertThat(dcs).hasSize(11);
    Iterator<DataCenterSpec> it = dcs.iterator();
    for (int i = 0; i < 11; i++) {
      DataCenterSpec dc = it.next();
      assertThat(dc.getNodes()).hasSize(i);
      assertThat(dc.getId()).isEqualTo(i);
    }
  }

  @Test
  public void testLookupDCAndNode() {
    ClusterSpec cluster = ClusterSpec.builder().withId(0L).withNodes(5, 5).build();

    // Lookup dc by id
    assertThat(cluster.dc(2)).isNull();
    assertThat(cluster.dc(0).getId()).isEqualTo(0);
    assertThat(cluster.dc(1).getId()).isEqualTo(1);

    // Lookup node by id
    assertThat(cluster.node(0, 1).resolveId()).isEqualTo("0:0:1");
    assertThat(cluster.node(0, 4).resolveId()).isEqualTo("0:0:4");
    assertThat(cluster.node(1, 4).resolveId()).isEqualTo("0:1:4");
    assertThat(cluster.node(1, 5)).isNull(); // no node in dc.
    assertThat(cluster.node(3, 1)).isNull(); // no dc in cluster.

    // dc then node
    assertThat(cluster.dc(0).node(1).resolveId()).isEqualTo("0:0:1");
  }

  @Test
  public void testCopy() {
    ClusterSpec cluster =
        ClusterSpec.builder()
            .withId(1L)
            .withName("cluster0")
            .withCassandraVersion("3.0.11")
            .withDSEVersion("5.1.0")
            .withPeerInfo("hello", "world")
            .build();

    cluster.addDataCenter().build();
    cluster.addDataCenter().build();

    ClusterSpec cluster2 = ClusterSpec.builder().copy(cluster).build();
    assertThat(cluster2.getId()).isEqualTo(cluster.getId());
    assertThat(cluster2.getName()).isEqualTo(cluster.getName());
    assertThat(cluster2.getCassandraVersion()).isEqualTo(cluster.getCassandraVersion());
    assertThat(cluster2.getPeerInfo()).isEqualTo(cluster.getPeerInfo());

    // DataCenters should not be copied.
    assertThat(cluster2.getDataCenters()).isEmpty();
  }
}
