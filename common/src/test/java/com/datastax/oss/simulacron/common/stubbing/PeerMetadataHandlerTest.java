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
package com.datastax.oss.simulacron.common.stubbing;

import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.INVALID;
import static com.datastax.oss.simulacron.common.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.DataCenterSpec;
import com.datastax.oss.simulacron.common.cluster.NodeSpec;
import com.datastax.oss.simulacron.common.utils.FrameUtils;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class PeerMetadataHandlerTest {

  // A 200 node cluster with 2 dcs with 100 nodes in each.
  private static final ClusterSpec cluster;
  private static NodeSpec node0;
  private static NodeSpec node1;

  // A 3 node cluster mimicking DSE 5.1.
  private static final ClusterSpec dseCluster;
  private static final NodeSpec dseNode0;

  private final PeerMetadataHandler handler = new PeerMetadataHandler();
  private final PeerMetadataHandler handlerV2 = new PeerMetadataHandler(true);

  static {
    cluster = ClusterSpec.builder().withName("cluster0").build();
    dseCluster = ClusterSpec.builder().withName("dseCluster0").withDSEVersion("5.0.8").build();
    DataCenterSpec dseDc0 = dseCluster.addDataCenter().withName("dseDc0").build();
    dseNode0 = dseDc0.addNode().withPeerInfo("graph", true).build();
    dseDc0.addNode().build();
    dseDc0.addNode().build();

    DataCenterSpec dc0 = cluster.addDataCenter().withName("dc0").build();
    DataCenterSpec dc1 = cluster.addDataCenter().withName("dc1").build();
    try {
      for (int i = 0; i < 100; i++) {
        NodeSpec node =
            dc0.addNode()
                .withAddress(
                    new InetSocketAddress(
                        InetAddress.getByAddress(new byte[] {127, 0, 10, (byte) i}), 9042))
                .build();
        if (i == 0) {
          node0 = node;
        }
      }
      for (int i = 0; i < 100; i++) {
        NodeSpec node =
            dc1.addNode()
                .withAddress(
                    new InetSocketAddress(
                        InetAddress.getByAddress(new byte[] {127, 0, 11, (byte) i}), 9042))
                .build();
        if (i == 0) {
          node1 = node;
        }
      }
    } catch (UnknownHostException e) {
      throw new RuntimeException("Could not assign addresses, this shouldn't happen");
    }
  }

  private static Frame queryFrame(String queryString) {
    return FrameUtils.wrapRequest(new Query(queryString));
  }

  private static Frame queryFrame(String queryString, QueryOptions options) {
    return FrameUtils.wrapRequest(new Query(queryString, options));
  }

  @Test
  public void shouldMatchLocalAndPeersQueries() {
    // Should match the following queries.
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.peers"))).isTrue();
    assertThat(handler.matches(node0, queryFrame(" select  *  from  system.peers  ; "))).isTrue();
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.peers_v2"))).isTrue();
    assertThat(handler.matches(node0, queryFrame(" select  *  from  system.peers_v2  ; ")))
        .isTrue();
    assertThat(handler.matches(node0, queryFrame("SELECT cluster_name FROM system.local")))
        .isTrue();
    assertThat(handler.matches(node0, queryFrame(" select  cluster_name  from  system.local ; ")))
        .isTrue();
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.local WHERE key='local'")))
        .isTrue();
    assertThat(
            handler.matches(
                node0, queryFrame(" select  *  from  system.local  where  key = 'local' ; ")))
        .isTrue();
    // Should match individual peer query no matter the address.
    assertThat(
            handler.matches(node0, queryFrame("SELECT * FROM system.peers WHERE peer='127.0.3.2'")))
        .isTrue();
    assertThat(
            handler.matches(
                node0, queryFrame(" select  *  from  system.peers  where  peer = '127.0.3.2' ; ")))
        .isTrue();
    assertThat(
            handler.matches(node0, queryFrame("SELECT * FROM system.peers WHERE peer='17.0.3.7'")))
        .isTrue();
    assertThat(
            handler.matches(
                node0, queryFrame(" select  *  from  system.peers  where  peer = '17.0.3.7' ; ")))
        .isTrue();
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.peers WHERE peer=:address")))
        .isTrue();
    assertThat(
            handler.matches(
                node0, queryFrame(" select  *  from  system.peers  where  peer = :address ; ")))
        .isTrue();
    assertThat(
            handler.matches(
                node0,
                queryFrame(
                    "SELECT * FROM system.peers_v2 WHERE peer=:address AND peer_port=:port")))
        .isFalse();
    assertThat(
            handler.matches(
                node0,
                queryFrame(
                    " select  *  from  system.peers_v2  where  peer = :address  and  peer_port = :port ; ")))
        .isFalse();
    assertThat(
            handlerV2.matches(
                node0,
                queryFrame(
                    "SELECT * FROM system.peers_v2 WHERE peer=:address AND peer_port=:port")))
        .isTrue();
    assertThat(
            handlerV2.matches(
                node0,
                queryFrame(
                    " select  *  from  system.peers_v2  where  peer = :address  and  peer_port = :port ; ")))
        .isTrue();
  }

  @Test
  public void shouldNotMatch() {
    // Should not match queries that aren't peer related.
    assertThat(handler.matches(node0, queryFrame("SELECT foo FROM bar"))).isFalse();
    // Should not match non-queries
    assertThat(handler.matches(node0, FrameUtils.wrapRequest(new Startup()))).isFalse();
    assertThat(handler.matches(node0, FrameUtils.wrapRequest(Options.INSTANCE))).isFalse();
  }

  @Test
  public void shouldReturnNoActionsForNonMatchingQuery() {
    // Should not return any actions if the query doesn't match.
    assertThat(handler.getActions(node0, FrameUtils.wrapRequest(new Startup()))).isEmpty();
  }

  @Test
  public void shouldHandleQueryLocal() {
    // querying the local table should return node info.
    List<Action> node0Actions =
        handler.getActions(node0, queryFrame("SELECT * FROM system.local WHERE key='local'"));

    assertThat(node0Actions).hasSize(1);

    Action node0Action = node0Actions.get(0);
    assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

    Message node0Message = ((MessageResponseAction) node0Action).getMessage();
    assertThat(node0Message)
        .isRows()
        .hasRows(1)
        .hasColumnSpecs(17)
        .hasColumn(0, 0, "local")
        .hasColumn(0, 1, "COMPLETED")
        .hasColumn(0, 2, ((InetSocketAddress) node0.getAddress()).getAddress())
        .hasColumn(0, 3, ((InetSocketAddress) node0.getAddress()).getPort())
        .hasColumn(0, 4, ((InetSocketAddress) node0.getAddress()).getAddress())
        .hasColumn(0, 5, ((InetSocketAddress) node0.getAddress()).getPort())
        .hasColumn(0, 6, cluster.getName())
        .hasColumn(0, 7, "3.2.0")
        .hasColumn(0, 8, node0.getDataCenter().getName())
        .hasColumn(0, 9, ((InetSocketAddress) node0.getAddress()).getAddress())
        .hasColumn(0, 10, ((InetSocketAddress) node0.getAddress()).getPort())
        .hasColumn(0, 11, "org.apache.cassandra.dht.Murmur3Partitioner")
        .hasColumn(0, 12, "rack1")
        .hasColumn(0, 13, "3.0.12")
        .hasColumn(0, 14, Collections.singleton("0"))
        .hasColumn(0, 15, node0.getHostId())
        .hasColumn(0, 16, PeerMetadataHandler.schemaVersion);
  }

  @Test
  public void shouldHandleQueryLocalDSE() {
    // querying the local table should return node info and include dse specific columns
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.local WHERE key='local'",
            "select * from system.local where key='local'",
            " select * from system.local where key = 'local' ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(dseNode0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();
      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(19) // should include dse_version and graph columns
          .hasColumn(0, 0, "local")
          .hasColumn(0, 1, "COMPLETED")
          .hasColumn(0, 2, InetAddress.getLoopbackAddress())
          .hasColumn(0, 3, 9042)
          .hasColumn(0, 4, InetAddress.getLoopbackAddress())
          .hasColumn(0, 5, 9042)
          .hasColumn(0, 6, dseCluster.getName())
          .hasColumn(0, 7, "3.2.0")
          .hasColumn(0, 8, dseNode0.getDataCenter().getName())
          .hasColumn(0, 9, InetAddress.getLoopbackAddress())
          .hasColumn(0, 10, 9042)
          .hasColumn(0, 11, "org.apache.cassandra.dht.Murmur3Partitioner")
          .hasColumn(0, 12, "rack1")
          .hasColumn(0, 13, "3.0.12")
          .hasColumn(0, 14, Collections.singleton("0"))
          .hasColumn(0, 15, dseNode0.getHostId())
          .hasColumn(0, 16, PeerMetadataHandler.schemaVersion)
          .hasColumn(0, 17, "5.0.8")
          .hasColumn(0, 18, true);
    }
  }

  @Test
  public void shouldHandleQueryLocalNode1() {
    // querying the local table should return node info for node1
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.local WHERE key='local'",
            "select * from system.local where key='local'",
            " select * from system.local where key = 'local' ; ");

    for (String query : queries) {
      List<Action> node1Actions = handler.getActions(node1, queryFrame(query));

      assertThat(node1Actions).hasSize(1);

      Action node1Action = node1Actions.get(0);
      assertThat(node1Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node1Action).getMessage();
      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(17)
          .hasColumn(0, 0, "local")
          .hasColumn(0, 1, "COMPLETED")
          .hasColumn(0, 2, ((InetSocketAddress) node1.getAddress()).getAddress())
          .hasColumn(0, 3, ((InetSocketAddress) node1.getAddress()).getPort())
          .hasColumn(0, 4, ((InetSocketAddress) node1.getAddress()).getAddress())
          .hasColumn(0, 5, ((InetSocketAddress) node1.getAddress()).getPort())
          .hasColumn(0, 6, cluster.getName())
          .hasColumn(0, 7, "3.2.0")
          .hasColumn(0, 8, node1.getDataCenter().getName())
          .hasColumn(0, 9, ((InetSocketAddress) node1.getAddress()).getAddress())
          .hasColumn(0, 10, ((InetSocketAddress) node1.getAddress()).getPort())
          .hasColumn(0, 11, "org.apache.cassandra.dht.Murmur3Partitioner")
          .hasColumn(0, 12, "rack1")
          .hasColumn(0, 13, "3.0.12")
          .hasColumn(0, 14, Collections.singleton("0"))
          .hasColumn(0, 15, node1.getHostId())
          .hasColumn(0, 16, PeerMetadataHandler.schemaVersion);
    }
  }

  @Test
  public void shouldHandleQueryClusterName() {
    // querying the local table for cluster_name should return ClusterSpec.getName()
    List<String> queries =
        Arrays.asList(
            "SELECT cluster_name FROM system.local",
            "select cluster_name from system.local where key='local'",
            "select cluster_name from system.local where key = 'local'");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(1)
          .hasColumn(0, 0, cluster.getName());
    }
  }

  @Test
  public void shouldHandleQueryAllPeers() {
    // querying for peers should return a row for each other node in the cluster
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers",
            "select * from system.peers",
            " select  *  from  system.peers ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 199 peers (200 node cluster - 1 for this node).
      assertThat(node0Message).isRows().hasRows(199).hasColumnSpecs(8);
    }
  }

  @Test
  public void shouldHandleQueryAllPeersV2() {
    // querying for peers should return a row for each other node in the cluster
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers_v2",
            "select * from system.peers_v2",
            " select  *  from  system.peers_v2 ; ");

    for (String query : queries) {
      List<Action> node0Actions = handlerV2.getActions(node0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 199 peers (200 node cluster - 1 for this node).
      assertThat(node0Message).isRows().hasRows(199).hasColumnSpecs(10);
    }
  }

  @Test
  public void shouldNotHandleQueryAllPeersV2WhenV2NotSupported() {
    // querying for peers using v2 should return an error when v2 is not supported.
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers_v2",
            "select * from system.peers_v2",
            " select  *  from  system.peers_v2 ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // Should get an invalid error when querying v2 table and v2 is not supported.
      assertThat(node0Message).isError(INVALID);
    }
  }

  @Test
  public void shouldHandleQueryAllPeersDSE() {
    // querying for peers should return a row for each other node in the cluster and return DSE
    // columns
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers",
            "select * from system.peers",
            " select  *  from  system.peers ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(dseNode0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 2 peers and 10 columns (2 extra for dse)
      assertThat(node0Message).isRows().hasRows(2).hasColumnSpecs(10);
    }
  }

  @Test
  public void shouldHandleQuerySpecificPeer() throws UnknownHostException {
    // when peer query is made for a peer in the cluster, we should get 1 row back.
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers WHERE peer='127.0.11.17'",
            "select * from system.peers where peer = '127.0.11.17'",
            " select  *  from  system.peers  where  peer  =  '127.0.11.17' ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 1 matching peer
      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(8)
          .hasColumn(0, 0, InetAddress.getByAddress(new byte[] {127, 0, 11, 17}))
          .hasColumn(0, 1, "dc1");
    }
  }

  @Test
  public void shouldHandleQuerySpecificPeerNamedParameter() throws UnknownHostException {
    // when peer query is made for a peer in the cluster using named parameters, we should get 1 row
    // back.
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 11, 17});
    Map<String, ByteBuffer> params = new HashMap<>();
    params.put("address", ByteBuffer.wrap(addr.getAddress()));
    QueryOptions queryOptions =
        new QueryOptions(
            0,
            Collections.emptyList(),
            params,
            false,
            0,
            null,
            10,
            Long.MIN_VALUE,
            null,
            Integer.MIN_VALUE);
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers WHERE peer=:address",
            "select * from system.peers where peer = :address",
            " select  *  from  system.peers  where  peer = :address ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query, queryOptions));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 1 matching peer
      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(8)
          .hasColumn(0, 0, addr)
          .hasColumn(0, 1, "dc1");
    }
  }

  @Test
  public void shouldHandleQuerySpecificPeersV2NamedParameter() throws UnknownHostException {
    // when peer query is made for a peer in the cluster using named parameters, we should get 1 row
    // back.
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 11, 17});
    Map<String, ByteBuffer> params = new HashMap<>();
    params.put("address", ByteBuffer.wrap(addr.getAddress()));
    ByteBuffer port = ByteBuffer.allocate(4);
    port.putInt(9042);
    port.flip();
    params.put("port", port);
    QueryOptions queryOptions =
        new QueryOptions(
            0,
            Collections.emptyList(),
            params,
            false,
            0,
            null,
            10,
            Long.MIN_VALUE,
            null,
            Integer.MIN_VALUE);
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers_v2 WHERE peer=:address AND peer_port=:port",
            "select * from system.peers_v2 where peer = :address and peer_port = :port",
            " select  *  from  system.peers_v2  where  peer = :address  and  peer_port = :port ; ");

    for (String query : queries) {
      List<Action> node0Actions = handlerV2.getActions(node0, queryFrame(query, queryOptions));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should be 1 matching peer
      assertThat(node0Message)
          .isRows()
          .hasRows(1)
          .hasColumnSpecs(10)
          .hasColumn(0, 0, addr)
          .hasColumn(0, 9, 9042);
    }
  }

  @Test
  public void shouldNotHandleQuerySpecificPeersV2NamedParameter() throws UnknownHostException {
    // when peers_v2 query is made, an invalid error should be raised is the handler does not
    // support v2.
    InetAddress addr = InetAddress.getByAddress(new byte[] {127, 0, 11, 17});
    Map<String, ByteBuffer> params = new HashMap<>();
    params.put("address", ByteBuffer.wrap(addr.getAddress()));
    ByteBuffer port = ByteBuffer.allocate(4);
    port.putInt(9042);
    port.flip();
    params.put("port", port);
    QueryOptions queryOptions =
        new QueryOptions(
            0,
            Collections.emptyList(),
            params,
            false,
            0,
            null,
            10,
            Long.MIN_VALUE,
            null,
            Integer.MIN_VALUE);
    List<String> queries =
        Arrays.asList(
            "SELECT * FROM system.peers_v2 WHERE peer=:address AND peer_port=:port",
            "select * from system.peers_v2 where peer = :address and peer_port = :port",
            " select  *  from  system.peers_v2  where  peer = :address  and  peer_port = :port ; ");

    for (String query : queries) {
      List<Action> node0Actions = handler.getActions(node0, queryFrame(query, queryOptions));

      assertThat(node0Actions).hasSize(1);

      Action node0Action = node0Actions.get(0);
      assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

      Message node0Message = ((MessageResponseAction) node0Action).getMessage();

      // should return an error.
      assertThat(node0Message).isError(INVALID);
    }
  }

  @Test
  public void shouldHandleQuerySpecificPeerNotFound() {
    // when peer query is made for a peer not in the cluster, we should get 0 rows back.
    List<Action> node0Actions =
        handler.getActions(
            node0, queryFrame("SELECT * FROM system.peers WHERE peer='127.0.12.17'"));

    assertThat(node0Actions).hasSize(1);

    Action node0Action = node0Actions.get(0);
    assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

    Message node0Message = ((MessageResponseAction) node0Action).getMessage();

    // should be no rows since no peer matched.
    assertThat(node0Message).isRows().hasRows(0).hasColumnSpecs(8);
  }
}
