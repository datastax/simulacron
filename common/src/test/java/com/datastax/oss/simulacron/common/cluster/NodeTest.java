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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class NodeTest {

  @Test
  public void testShouldSetDCToParent() {
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dataCenter = cluster.addDataCenter().build();
    NodeSpec node = dataCenter.addNode().build();
    assertThat(node.getDataCenter()).isSameAs(dataCenter);
    assertThat(node.getCluster()).isSameAs(cluster);
    assertThat(node.getParent()).isEqualTo(Optional.of(dataCenter));
  }

  @Test
  public void testStandalone() {
    NodeSpec node = NodeSpec.builder().build();
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
    NodeSpec node =
        NodeSpec.builder()
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
    NodeSpec node = new NodeSpec();
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
    ClusterSpec cluster = ClusterSpec.builder().build();
    DataCenterSpec dc = cluster.addDataCenter().build();

    NodeSpec node =
        dc.addNode()
            .withName("node2")
            .withCassandraVersion("1.2.19")
            .withDSEVersion("5.1.0")
            .withPeerInfo("hello", "world")
            .withAddress(new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042))
            .build();

    NodeSpec node2 = dc.addNode().copy(node).build();
    assertThat(node2.getId()).isEqualTo(node.getId());
    assertThat(node2.getName()).isEqualTo(node.getName());
    assertThat(node2.getCassandraVersion()).isEqualTo(node.getCassandraVersion());
    assertThat(node2.getPeerInfo()).isEqualTo(node.getPeerInfo());

    // Address should not be copied.
    assertThat(node2.getAddress()).isNull();
  }
}
