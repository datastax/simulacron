package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.simulacron.common.cluster.DataCenter;
import com.datastax.simulacron.common.cluster.Node;
import com.datastax.simulacron.common.codec.Codec;
import com.datastax.simulacron.common.codec.CodecUtils;
import com.datastax.simulacron.common.codec.CqlMapper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.*;
import static com.datastax.simulacron.common.codec.CodecUtils.*;

public class PeerMetadataHandler implements StubMapping {

  private static final List<String> queries = new ArrayList<>();

  private static final UUID schemaVersion = java.util.UUID.randomUUID();

  private static final String queryPeers = "SELECT * FROM system.peers";
  private static final RowsMetadata querySystemPeersMetadata;
  private static final String queryClusterName = "select cluster_name from system.local";
  private static final RowsMetadata queryClusterNameMetadata;
  private static final String queryLocal = "SELECT * FROM system.local WHERE key='local'";
  private static final RowsMetadata queryLocalMetadata;

  static {
    ColumnSpecBuilder systemLocal = columnSpecBuilder("system", "local");
    List<ColumnSpec> queryLocalSpecs =
        columnSpecs(
            systemLocal.apply("key", primitive(ASCII)),
            systemLocal.apply("bootstrapped", primitive(ASCII)),
            systemLocal.apply("broadcast_address", primitive(INET)),
            systemLocal.apply("cluster_name", primitive(ASCII)),
            systemLocal.apply("cql_version", primitive(ASCII)),
            systemLocal.apply("data_center", primitive(ASCII)),
            systemLocal.apply("listen_address", primitive(INET)),
            systemLocal.apply("partitioner", primitive(ASCII)),
            systemLocal.apply("rack", primitive(ASCII)),
            systemLocal.apply("release_version", primitive(ASCII)),
            systemLocal.apply("tokens", new RawType.RawSet(primitive(ASCII))),
            systemLocal.apply("schema_version", primitive(UUID)));
    queryLocalMetadata = new RowsMetadata(queryLocalSpecs, null, new int[] {0});

    List<ColumnSpec> queryClusterNameSpecs =
        columnSpecs(systemLocal.apply("cluster_name", primitive(ASCII)));
    queryClusterNameMetadata = new RowsMetadata(queryClusterNameSpecs, null, new int[] {});

    ColumnSpecBuilder systemPeers = columnSpecBuilder("system", "peers");
    List<ColumnSpec> querySystemPeersSpecs =
        columnSpecs(
            systemPeers.apply("peer", primitive(INET)),
            systemPeers.apply("rpc_address", primitive(INET)),
            systemPeers.apply("data_center", primitive(ASCII)),
            systemPeers.apply("rack", primitive(ASCII)),
            systemPeers.apply("release_version", primitive(ASCII)),
            systemPeers.apply("tokens", new RawType.RawSet(primitive(ASCII))),
            systemPeers.apply("listen_address", primitive(INET)),
            systemPeers.apply("host_id", primitive(UUID)),
            systemPeers.apply("schema_version", primitive(UUID)));
    querySystemPeersMetadata = new RowsMetadata(querySystemPeersSpecs, null, new int[] {0});
  }

  private static final Pattern queryPeersWithAddr =
      Pattern.compile("SELECT \\* FROM system\\.peers WHERE peer='(.*)'");

  static {
    queries.add(queryPeers);
    queries.add(queryClusterName);
    queries.add(queryLocal);
  }

  public PeerMetadataHandler() {}

  @Override
  public boolean matches(Node node, Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      String queryStr = query.query;
      return queries.contains(queryStr) || queryPeersWithAddr.matcher(queryStr).matches();
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    if (frame.message instanceof Query) {
      CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
      Query query = (Query) frame.message;

      switch (query.query) {
        case queryLocal:
          return handleSystemLocalQuery(node, mapper);
        case queryClusterName:
          return handleClusterNameQuery(node, mapper);
        case queryPeers:
          return handlePeersQuery(node, mapper, n -> n != node);
        default:
          // if querying for particular peer, return information for only that peer.
          Matcher matcher = queryPeersWithAddr.matcher(query.query);
          if (matcher.matches()) {
            return handlePeersQuery(
                node,
                mapper,
                n -> {
                  InetAddress address;
                  if (n.getAddress() instanceof InetSocketAddress) {
                    address = ((InetSocketAddress) n.getAddress()).getAddress();
                    String addrIp = address.getHostAddress();
                    return addrIp.equals(matcher.group(1));
                  } else {
                    return false;
                  }
                });
          }
      }
    }
    return Collections.emptyList();
  }

  List<Action> handleSystemLocalQuery(Node node, CqlMapper mapper) {
    InetAddress address = resolveAddress(node);
    Codec<Set<String>> tokenCodec = mapper.codecFor(new RawType.RawSet(primitive(ASCII)));

    Queue<List<ByteBuffer>> localRow =
        CodecUtils.singletonRow(
            encodePeerInfo(node, mapper.ascii::encode, "key", "local"),
            encodePeerInfo(node, mapper.ascii::encode, "bootstrapped", "COMPLETED"),
            mapper.inet.encode(address),
            mapper.ascii.encode(node.getCluster().getName()),
            encodePeerInfo(node, mapper.ascii::encode, "cql_version", "3.2.0"),
            mapper.ascii.encode(node.getDataCenter().getName()),
            mapper.inet.encode(address),
            encodePeerInfo(
                node,
                mapper.ascii::encode,
                "partitioner",
                "org.apache.cassandra.dht.Murmur3Partitioner"),
            encodePeerInfo(node, mapper.ascii::encode, "rack", "rack1"),
            mapper.ascii.encode(node.resolveCassandraVersion()),
            tokenCodec.encode(Collections.singleton(resolveToken(node))),
            mapper.uuid.encode(schemaVersion));
    Rows rows = new Rows(queryLocalMetadata, localRow);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  List<Action> handleClusterNameQuery(Node node, CqlMapper mapper) {
    Queue<List<ByteBuffer>> clusterRow =
        CodecUtils.singletonRow(mapper.ascii.encode(node.getCluster().getName()));
    Rows rows = new Rows(queryClusterNameMetadata, clusterRow);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  List<Action> handlePeersQuery(Node node, CqlMapper mapper, Predicate<Node> nodeFilter) {
    // For each node matching the filter, provide its peer information.
    Codec<Set<String>> tokenCodec = mapper.codecFor(new RawType.RawSet(primitive(ASCII)));
    Queue<List<ByteBuffer>> peerRows =
        new ArrayDeque<>(
            node.getCluster()
                .getNodes()
                .stream()
                .filter(nodeFilter)
                .map(
                    n -> {
                      InetAddress address = resolveAddress(n);

                      return row(
                          mapper.inet.encode(address),
                          mapper.inet.encode(address),
                          mapper.varchar.encode(n.getDataCenter().getName()),
                          encodePeerInfo(n, mapper.varchar::encode, "rack", "rack1"),
                          mapper.varchar.encode(n.resolveCassandraVersion()),
                          tokenCodec.encode(Collections.singleton(resolveToken(n))),
                          mapper.inet.encode(address),
                          mapper.uuid.encode(schemaVersion),
                          mapper.uuid.encode(schemaVersion));
                    })
                .collect(Collectors.toList()));

    Rows rows = new Rows(querySystemPeersMetadata, peerRows);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  String resolveToken(Node node) {
    Optional<Object> token = node.resolvePeerInfo("token");
    if (token.isPresent()) {
      return token.toString();
    } else {
      // Determine token based on node's position in ring.
      // First identify position of datacenter in cluster.
      DataCenter dc = node.getDataCenter();
      int dcPos = 0;
      int nPos = 0;
      for (DataCenter d : node.getCluster().getDataCenters()) {
        if (d == dc) {
          break;
        }
        dcPos++;
      }
      // Identify node's position in dc.
      for (Node n : node.getDataCenter().getNodes()) {
        if (n == node) {
          break;
        }
        nPos++;
      }
      long dcOffset = dcPos * 100;
      long nodeOffset = nPos * ((long) Math.pow(2, 64) / node.getDataCenter().getNodes().size());
      return "" + (nodeOffset + dcOffset);
    }
  }

  private InetAddress resolveAddress(Node node) {
    InetAddress address;
    if (node.getAddress() instanceof InetSocketAddress) {
      address = ((InetSocketAddress) node.getAddress()).getAddress();
    } else {
      address = InetAddress.getLoopbackAddress();
    }
    return address;
  }
}
