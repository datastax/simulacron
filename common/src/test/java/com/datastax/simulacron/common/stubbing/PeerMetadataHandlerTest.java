package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.Message;
import com.datastax.oss.protocol.internal.request.Options;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.simulacron.common.cluster.Cluster;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.utils.FrameUtils;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class PeerMetadataHandlerTest {

  // A 200 node cluster with 2 dcs with 100 nodes in each.
  private static Cluster cluster;
  private static Node node0;
  private static Node node1;

  private PeerMetadataHandler handler = new PeerMetadataHandler();

  static {
    cluster = Cluster.builder().withName("cluster0").build();
    DataCenter dc0 = cluster.addDataCenter().withName("dc0").build();
    DataCenter dc1 = cluster.addDataCenter().withName("dc1").build();
    try {
      for (int i = 0; i < 100; i++) {
        Node node =
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
        Node node =
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

  @Test
  public void shouldMatchLocalAndPeersQueries() {
    // Should match the following queries.
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.peers"))).isTrue();
    assertThat(handler.matches(node0, queryFrame("select cluster_name from system.local")))
        .isTrue();
    assertThat(handler.matches(node0, queryFrame("SELECT * FROM system.local WHERE key='local'")))
        .isTrue();
    // Should match individual peer query no matter the address.
    assertThat(
            handler.matches(node0, queryFrame("SELECT * FROM system.peers WHERE peer='127.0.3.2'")))
        .isTrue();
    assertThat(
            handler.matches(node0, queryFrame("SELECT * FROM system.peers WHERE peer='17.0.3.7'")))
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
    assertThat(node0Message).isInstanceOf(Rows.class);

    Rows rows = (Rows) node0Message;
    assertThat(rows.data).hasSize(1);
    assertThat(rows.metadata.columnSpecs).hasSize(12);

    // TODO: Validate the actual data, need decoders for this.
  }

  @Test
  public void shouldHandleQueryClusterName() {
    // querying the local table for cluster_name should return Cluster.getName()
    List<Action> node0Actions =
        handler.getActions(node0, queryFrame("select cluster_name from system.local"));

    assertThat(node0Actions).hasSize(1);

    Action node0Action = node0Actions.get(0);
    assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

    Message node0Message = ((MessageResponseAction) node0Action).getMessage();
    assertThat(node0Message).isInstanceOf(Rows.class);

    Rows rows = (Rows) node0Message;
    assertThat(rows.data).hasSize(1);
    assertThat(rows.metadata.columnSpecs).hasSize(1);

    // TODO: Validate the actual data, need decoders for this.
  }

  @Test
  public void shouldHandleQueryAllPeers() {
    // querying for peers should return a row for each other node in the cluster
    List<Action> node0Actions = handler.getActions(node0, queryFrame("SELECT * FROM system.peers"));

    assertThat(node0Actions).hasSize(1);

    Action node0Action = node0Actions.get(0);
    assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

    Message node0Message = ((MessageResponseAction) node0Action).getMessage();
    assertThat(node0Message).isInstanceOf(Rows.class);

    Rows rows = (Rows) node0Message;
    // should be 199 peers (200 node cluster - 1 for this node).
    assertThat(rows.data).hasSize(199);
    assertThat(rows.metadata.columnSpecs).hasSize(9);

    // TODO: Validate the actual data, need decoders for this.
  }

  @Test
  public void shouldHandleQuerySpecificPeer() {
    // when peer query is made for a peer in the cluster, we should get 1 row back.
    List<Action> node0Actions =
        handler.getActions(
            node0, queryFrame("SELECT * FROM system.peers WHERE peer='127.0.11.17'"));

    assertThat(node0Actions).hasSize(1);

    Action node0Action = node0Actions.get(0);
    assertThat(node0Action).isInstanceOf(MessageResponseAction.class);

    Message node0Message = ((MessageResponseAction) node0Action).getMessage();
    assertThat(node0Message).isInstanceOf(Rows.class);

    Rows rows = (Rows) node0Message;
    // should be 1 matching peer
    assertThat(rows.data).hasSize(1);
    assertThat(rows.metadata.columnSpecs).hasSize(9);

    // TODO: Validate the actual data, need decoders for this.

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
    assertThat(node0Message).isInstanceOf(Rows.class);

    Rows rows = (Rows) node0Message;
    // should be 0 matching peers.
    assertThat(rows.data).hasSize(0);
    assertThat(rows.metadata.columnSpecs).hasSize(9);
  }
}
