package com.datastax.simulacron.common.stubbing;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
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

public class PeerMetadataHandler extends StubMapping implements InternalStubMapping {

  private static final List<String> queries = new ArrayList<>();
  private static final List<Pattern> queryPatterns = new ArrayList<>();

  static final UUID schemaVersion = java.util.UUID.randomUUID();

  private static final String queryClusterName = "select cluster_name from system.local";
  private static final RowsMetadata queryClusterNameMetadata;

  static {
    ColumnSpecBuilder systemLocal = columnSpecBuilder("system", "local");
    List<ColumnSpec> queryClusterNameSpecs =
        columnSpecs(systemLocal.apply("cluster_name", primitive(ASCII)));
    queryClusterNameMetadata = new RowsMetadata(queryClusterNameSpecs, null, new int[] {});
  }

  private static final Pattern queryPeers = Pattern.compile("SELECT (.*) FROM system\\.peers");
  private static final Pattern queryLocal =
      Pattern.compile("SELECT (.*) FROM system\\.local WHERE key='local'");
  private static final Pattern queryPeersWithAddr =
      Pattern.compile("SELECT \\* FROM system\\.peers WHERE peer='(.*)'");

  static {
    queries.add(queryClusterName);
  }

  static {
    queryPatterns.add(queryPeers);
    queryPatterns.add(queryLocal);
    queryPatterns.add(queryPeersWithAddr);
  }

  public PeerMetadataHandler() {}

  @Override
  public boolean matches(Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      String queryStr = query.query;
      return queries.contains(queryStr) || queryPatternMatches(queryStr);
    }
    return false;
  }

  @Override
  public List<Action> getActions(Node node, Frame frame) {
    if (frame.message instanceof Query) {
      CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
      Query query = (Query) frame.message;

      if (query.query.equals(queryClusterName)) {
        return handleClusterNameQuery(node, mapper);
      } else {
        // if querying for particular peer, return information for only that peer.
        final Matcher peerAddrMatcher = queryPeersWithAddr.matcher(query.query);
        if (peerAddrMatcher.matches()) {
          return handlePeersQuery(
              node,
              mapper,
              n -> {
                InetAddress address;
                if (n.getAddress() instanceof InetSocketAddress) {
                  address = ((InetSocketAddress) n.getAddress()).getAddress();
                  String addrIp = address.getHostAddress();
                  return addrIp.equals(peerAddrMatcher.group(1));
                } else {
                  return false;
                }
              });
        }
        Matcher matcher = queryLocal.matcher(query.query);
        if (matcher.matches()) {
          return handleSystemLocalQuery(node, mapper);
        }
        matcher = queryPeers.matcher(query.query);
        if (matcher.matches()) {
          return handlePeersQuery(node, mapper, n -> n != node);
        }
      }
    }
    return Collections.emptyList();
  }

  private boolean queryPatternMatches(String query) {
    for (Pattern pattern : queryPatterns) {
      if (pattern.matcher(query).matches()) {
        return true;
      }
    }
    return false;
  }

  private Set<String> resolveTokens(Node node) {
    String[] t = node.resolvePeerInfo("token", "0").split(",");
    return new LinkedHashSet<>(Arrays.asList(t));
  }

  private List<Action> handleSystemLocalQuery(Node node, CqlMapper mapper) {
    InetAddress address = resolveAddress(node);
    Codec<Set<String>> tokenCodec = mapper.codecFor(new RawType.RawSet(primitive(ASCII)));

    List<ByteBuffer> localRow =
        CodecUtils.row(
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
            tokenCodec.encode(resolveTokens(node)),
            mapper.uuid.encode(schemaVersion));

    if (node.resolveDSEVersion() != null) {
      localRow.add(mapper.ascii.encode(node.resolveDSEVersion()));
      localRow.add(encodePeerInfo(node, mapper.bool::encode, "graph", false));
    }

    Rows rows = new Rows(buildSystemLocalRowsMetadata(node), CodecUtils.rows(localRow));
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  private List<Action> handleClusterNameQuery(Node node, CqlMapper mapper) {
    Queue<List<ByteBuffer>> clusterRow =
        CodecUtils.singletonRow(mapper.ascii.encode(node.getCluster().getName()));
    Rows rows = new Rows(queryClusterNameMetadata, clusterRow);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  private List<Action> handlePeersQuery(Node node, CqlMapper mapper, Predicate<Node> nodeFilter) {
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

                      List<ByteBuffer> row =
                          row(
                              mapper.inet.encode(n.resolvePeerInfo("peer", address)),
                              mapper.inet.encode(n.resolvePeerInfo("rpc_address", address)),
                              mapper.varchar.encode(
                                  n.resolvePeerInfo("data_center", n.getDataCenter().getName())),
                              encodePeerInfo(n, mapper.varchar::encode, "rack", "rack1"),
                              mapper.varchar.encode(
                                  n.resolvePeerInfo(
                                      "release_version", n.resolveCassandraVersion())),
                              tokenCodec.encode(n.resolvePeerInfo("tokens", resolveTokens(n))),
                              mapper.inet.encode(n.resolvePeerInfo("listen_address", address)),
                              mapper.uuid.encode(n.resolvePeerInfo("host_id", schemaVersion)),
                              mapper.uuid.encode(
                                  n.resolvePeerInfo("schema_version", schemaVersion)));
                      if (node.resolveDSEVersion() != null) {
                        row.add(mapper.ascii.encode(n.resolveDSEVersion()));
                        row.add(encodePeerInfo(n, mapper.bool::encode, "graph", false));
                      }
                      return row;
                    })
                .collect(Collectors.toList()));

    Rows rows = new Rows(buildSystemPeersRowsMetadata(node), peerRows);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
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

  private RowsMetadata buildSystemPeersRowsMetadata(Node node) {
    ColumnSpecBuilder systemPeers = columnSpecBuilder("system", "peers");
    List<ColumnSpec> systemPeersSpecs =
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
    if (node.resolveDSEVersion() != null) {
      systemPeersSpecs.add(systemPeers.apply("dse_version", primitive(ASCII)));
      systemPeersSpecs.add(systemPeers.apply("graph", primitive(BOOLEAN)));
    }
    return new RowsMetadata(systemPeersSpecs, null, new int[] {0});
  }

  private RowsMetadata buildSystemLocalRowsMetadata(Node node) {
    ColumnSpecBuilder systemLocal = columnSpecBuilder("system", "local");
    List<ColumnSpec> systemLocalSpecs =
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
    if (node.resolveDSEVersion() != null) {
      systemLocalSpecs.add(systemLocal.apply("dse_version", primitive(ASCII)));
      systemLocalSpecs.add(systemLocal.apply("graph", primitive(BOOLEAN)));
    }
    return new RowsMetadata(systemLocalSpecs, null, new int[] {0});
  }
}
