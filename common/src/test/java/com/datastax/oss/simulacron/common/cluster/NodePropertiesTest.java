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
package com.datastax.oss.simulacron.common.cluster;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Optional;
import org.junit.Test;

public class NodePropertiesTest {

  @Test
  public void testResolveDirect() {
    // should resolve values when they are set directly on their subjects.
    ClusterSpec cluster =
        ClusterSpec.builder()
            .withName("cluster0")
            .withId(1L)
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .build();

    DataCenterSpec dc =
        cluster
            .addDataCenter()
            .withName("dc0")
            .withCassandraVersion("2.0.17")
            .withPeerInfo("goodbye", "sun")
            .withPeerInfo("nothing", "something")
            .build();

    NodeSpec node =
        dc.addNode()
            .withName("node0")
            .withId(7L)
            .withCassandraVersion("3.0.11")
            .withPeerInfo("hola", "mundo")
            .withPeerInfo("nothing", null)
            .build();

    assertThat(node.resolveCassandraVersion()).isEqualTo("3.0.11");
    assertThat(node.resolveId()).isEqualTo("1:0:7");
    assertThat(node.resolveName()).isEqualTo("cluster0:dc0:node0");
    assertThat(node.resolvePeerInfo("hola", String.class)).isEqualTo(Optional.of("mundo"));
    assertThat(node.resolvePeerInfo("hola", "no")).isEqualTo("mundo");
    assertThat(node.resolvePeerInfo("yo", "hi")).isEqualTo("hi");

    // setting null explicitly on node should resolve it to null.
    assertThat(node.resolvePeerInfo("nothing", "defaultvalue")).isNull();
  }

  @Test
  public void testResolveIndirect() {
    // should resolve values when they are set directly on their subjects.
    ClusterSpec cluster =
        ClusterSpec.builder()
            .withName("cluster0")
            .withId(1L)
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .build();

    DataCenterSpec dc =
        cluster.addDataCenter().withName("dc0").withPeerInfo("goodbye", "sun").build();

    NodeSpec node = dc.addNode().withPeerInfo("hola", "mundo").build();

    assertThat(node.resolveCassandraVersion()).isEqualTo("1.2.19");
    // NodeSpec id/name should be resolved to 0 since first node in dc.
    assertThat(node.resolveId()).isEqualTo("1:0:0");
    assertThat(node.resolveName()).isEqualTo("cluster0:dc0:0");
    assertThat(node.resolvePeerInfo("hello", String.class)).isEqualTo(Optional.of("world"));
    assertThat(node.resolvePeerInfo("goodbye", String.class)).isEqualTo(Optional.of("sun"));
    assertThat(node.resolvePeerInfo("yo", "hi")).isEqualTo("hi");
  }

  @Test
  public void testToStringWithId() {
    NodeSpec node = NodeSpec.builder().withId(7L).withName("node0").build();

    assertThat(node.toString()).isEqualTo("NodeSpec{id=7, name='node0', address=null}");
  }

  @Test
  public void testToStringNoName() {
    NodeSpec node = NodeSpec.builder().withId(7L).build();

    assertThat(node.toString()).isEqualTo("NodeSpec{id=7, address=null}");
  }

  @Test
  public void testToStringWithProps() {
    NodeSpec node =
        NodeSpec.builder()
            .withId(7L)
            .withName("node0")
            .withPeerInfo("hello", "world")
            .withCassandraVersion("1.2.19")
            .withDSEVersion("5.1.0")
            .build();

    assertThat(node.toString())
        .isEqualTo(
            "NodeSpec{id=7, name='node0', cassandraVersion='1.2.19', dseVersion='5.1.0', peerInfo={hello=world}, address=null}");
  }
}
