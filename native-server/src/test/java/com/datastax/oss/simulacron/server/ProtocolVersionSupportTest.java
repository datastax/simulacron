/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.simulacron.server;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import io.netty.channel.local.LocalAddress;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import java.util.Collections;
import java.util.UUID;
import org.assertj.core.util.Lists;
import org.junit.Test;

public class ProtocolVersionSupportTest {

  private static final Timer timer = new HashedWheelTimer();

  @Test
  public void testShouldSetV3andV4ByDefault() {
    testProtocolVersionForCassandraVersion(null, 3, 4);
  }

  @Test
  public void testShouldSetV3ForCassandra21() {
    testProtocolVersionForCassandraVersion("2.1.17", 3);
  }

  @Test
  public void testShouldSetV3andV4ForCassandra22() {
    testProtocolVersionForCassandraVersion("2.2.11", 3, 4);
  }

  @Test
  public void testShouldSetV3andV4ForCassandra30() {
    testProtocolVersionForCassandraVersion("3.0.17", 3, 4);
  }

  @Test
  public void testShouldSetV3andV4andV5ForCassandra40() {
    testProtocolVersionForCassandraVersion("4.0.0", 3, 4, 5);
  }

  @Test
  public void shouldInheritClusterOverride() {
    BoundCluster cluster =
        new BoundCluster(
            ClusterSpec.builder().withPeerInfo("protocol_versions", Lists.newArrayList(5)).build(),
            0L,
            null);
    BoundDataCenter dc = new BoundDataCenter(cluster);
    BoundNode node =
        new BoundNode(
            new LocalAddress(UUID.randomUUID().toString()),
            NodeSpec.builder().withName("node0").withId(0L).build(),
            Collections.emptyMap(),
            cluster,
            dc,
            null,
            timer,
            null, // channel reference only needed for closing, not useful in context of this test.
            false);

    assertThat(node.getFrameCodec().getSupportedProtocolVersions()).containsOnly(5);
    assertThat(dc.getFrameCodec().getSupportedProtocolVersions()).containsOnly(5);
    assertThat(cluster.getFrameCodec().getSupportedProtocolVersions()).containsOnly(5);
  }

  @Test
  public void shouldInheritClusterOverrideFromCassandraVersion() {
    BoundCluster cluster =
        new BoundCluster(ClusterSpec.builder().withCassandraVersion("2.1.17").build(), 0L, null);
    BoundDataCenter dc = new BoundDataCenter(cluster);
    BoundNode node =
        new BoundNode(
            new LocalAddress(UUID.randomUUID().toString()),
            NodeSpec.builder().withName("node0").withId(0L).build(),
            Collections.emptyMap(),
            cluster,
            dc,
            null,
            timer,
            null, // channel reference only needed for closing, not useful in context of this test.
            false);

    assertThat(node.getFrameCodec().getSupportedProtocolVersions()).containsOnly(3);
    assertThat(dc.getFrameCodec().getSupportedProtocolVersions()).containsOnly(3);
    assertThat(cluster.getFrameCodec().getSupportedProtocolVersions()).containsOnly(3);
  }

  @Test
  public void shouldInheritDCOverride() {
    ClusterSpec clusterSpec =
        ClusterSpec.builder().withPeerInfo("protocol_versions", Lists.newArrayList(5)).build();
    BoundCluster cluster = new BoundCluster(clusterSpec, 0L, null);
    DataCenterSpec dcSpec =
        clusterSpec
            .addDataCenter()
            .withPeerInfo("protocol_versions", Lists.newArrayList(4))
            .build();
    BoundDataCenter dc = new BoundDataCenter(dcSpec, cluster);
    BoundNode node =
        new BoundNode(
            new LocalAddress(UUID.randomUUID().toString()),
            NodeSpec.builder().withName("node0").withId(0L).build(),
            Collections.emptyMap(),
            cluster,
            dc,
            null,
            timer,
            null, // channel reference only needed for closing, not useful in context of this test.
            false);

    assertThat(node.getFrameCodec().getSupportedProtocolVersions()).containsOnly(4);
    assertThat(dc.getFrameCodec().getSupportedProtocolVersions()).containsOnly(4);
    assertThat(cluster.getFrameCodec().getSupportedProtocolVersions()).containsOnly(5);
  }

  @Test
  public void testShouldUseProtocolVersionOverride() {
    BoundCluster cluster = new BoundCluster(ClusterSpec.builder().build(), 0L, null);
    BoundDataCenter dc = new BoundDataCenter(cluster);
    BoundNode node =
        new BoundNode(
            new LocalAddress(UUID.randomUUID().toString()),
            NodeSpec.builder()
                .withName("node0")
                .withId(0L)
                .withCassandraVersion("2.1.17")
                .withPeerInfo("protocol_versions", Lists.newArrayList(4))
                .build(),
            Collections.emptyMap(),
            cluster,
            dc,
            null,
            timer,
            null, // channel reference only needed for closing, not useful in context of this test.
            false);

    assertThat(node.getFrameCodec().getSupportedProtocolVersions()).containsOnly(4);
  }

  public void testProtocolVersionForCassandraVersion(
      String cassandraVersion, Integer... expectedProtocolVersions) {
    BoundCluster cluster = new BoundCluster(ClusterSpec.builder().build(), 0L, null);
    BoundDataCenter dc = new BoundDataCenter(cluster);
    BoundNode node =
        new BoundNode(
            new LocalAddress(UUID.randomUUID().toString()),
            NodeSpec.builder()
                .withName("node0")
                .withId(0L)
                .withCassandraVersion(cassandraVersion)
                .build(),
            Collections.emptyMap(),
            cluster,
            dc,
            null,
            timer,
            null, // channel reference only needed for closing, not useful in context of this test.
            false);

    assertThat(node.getFrameCodec().getSupportedProtocolVersions())
        .containsOnly(expectedProtocolVersions);
  }
}
