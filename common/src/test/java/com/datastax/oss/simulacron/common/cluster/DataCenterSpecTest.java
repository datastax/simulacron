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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class DataCenterSpecTest {

  @Test
  public void testShouldSetClusterToParent() {
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dataCenter = cluster.addDataCenter().build();
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
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc =
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
    DataCenterSpec dc = new DataCenterSpec();
    for (int i = 0; i < 5; i++) {
      NodeSpec node = dc.addNode().build();
      assertThat(node.getId()).isEqualTo((long) i);
      assertThat(node.getDataCenter()).isEqualTo(dc);
      assertThat(node.getParent()).isEqualTo(Optional.of(dc));
      assertThat(dc.getNodes()).contains(node);
    }
    assertThat(dc.getNodes()).hasSize(5);
  }

  @Test
  public void testDefaultConstructor() {
    // This is only used by jackson mapper, but ensure it has sane defaults and doesn't throw any
    // exceptions.
    DataCenterSpec dc = new DataCenterSpec();
    assertThat(dc.getCluster()).isNull();
    assertThat(dc.getParent()).isEmpty();
    assertThat(dc.getId()).isEqualTo(null);
    assertThat(dc.getCassandraVersion()).isEqualTo(null);
    assertThat(dc.getName()).isEqualTo(null);
    assertThat(dc.getPeerInfo()).isEqualTo(Collections.emptyMap());
    assertThat(dc.getNodes()).isEmpty();
    assertThat(dc.toString()).isNotNull();
  }

  @Test
  public void testLookupNode() {
    DataCenterSpec dc = new DataCenterSpec();
    dc.addNode().build();
    dc.addNode().build();
    dc.addNode().build();

    // Lookup node by id
    assertThat(dc.node(0).getId()).isEqualTo(0);
    assertThat(dc.node(1).getId()).isEqualTo(1);
    assertThat(dc.node(2).getId()).isEqualTo(2);
    assertThat(dc.node(3)).isNull();
  }

  @Test
  public void testCopy() {
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc =
        cluster
            .addDataCenter()
            .withName("dc2")
            .withCassandraVersion("1.2.19")
            .withDSEVersion("5.1.0")
            .withPeerInfo("hello", "world")
            .build();

    dc.addNode().build();
    dc.addNode().build();

    DataCenterSpec dc2 = cluster.addDataCenter().copy(dc).build();
    assertThat(dc2.getId()).isEqualTo(dc.getId());
    assertThat(dc2.getName()).isEqualTo(dc.getName());
    assertThat(dc2.getCassandraVersion()).isEqualTo(dc.getCassandraVersion());
    assertThat(dc2.getPeerInfo()).isEqualTo(dc.getPeerInfo());

    // Nodes should not be copied.
    assertThat(dc2.getNodes()).isEmpty();
  }
}
