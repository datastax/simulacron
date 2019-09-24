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
package com.datastax.oss.simulacron.common.stubbing;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.ErrorCode.INVALID;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.response.Error;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.DefaultRows;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.Rows;
import com.datastax.oss.protocol.internal.response.result.RowsMetadata;
import com.datastax.oss.simulacron.common.cluster.AbstractCluster;
import com.datastax.oss.simulacron.common.cluster.AbstractNode;
import com.datastax.oss.simulacron.common.codec.Codec;
import com.datastax.oss.simulacron.common.codec.CodecUtils;
import com.datastax.oss.simulacron.common.codec.CqlMapper;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PeerMetadataHandler extends StubMapping implements InternalStubMapping {

  private final List<Pattern> queryPatterns = new ArrayList<>();

  static final UUID schemaVersion = java.util.UUID.randomUUID();

  private static final Pattern queryClusterName =
      Pattern.compile(
          "SELECT\\s+cluster_name\\s+FROM\\s+system\\.local(\\s+WHERE\\s+key\\s*=\\s*'local')?\\s*;?\\s*",
          Pattern.CASE_INSENSITIVE);
  private static final RowsMetadata queryClusterNameMetadata;

  static {
    CodecUtils.ColumnSpecBuilder systemLocal = CodecUtils.columnSpecBuilder("system", "local");
    List<ColumnSpec> queryClusterNameSpecs =
        CodecUtils.columnSpecs(systemLocal.apply("cluster_name", CodecUtils.primitive(ASCII)));
    queryClusterNameMetadata = new RowsMetadata(queryClusterNameSpecs, null, new int[] {}, null);
  }

  private static final Pattern queryPeers =
      Pattern.compile(
          "\\s*SELECT\\s+(.*)\\s+FROM\\s+system\\.(peers\\S*)\\s*;?\\s*", Pattern.CASE_INSENSITIVE);
  private static final Pattern queryLocal =
      Pattern.compile(
          "\\s*SELECT\\s+(.*)\\s+FROM\\s+system\\.local(\\s+WHERE\\s+key\\s*=\\s*'local')?\\s*;?\\s*",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern queryPeersWithAddr =
      Pattern.compile(
          "\\s*SELECT\\s+\\*\\s+FROM\\s+system\\.peers\\s+WHERE\\s+peer\\s*=\\s*'(.*)'\\s*;?\\s*",
          Pattern.CASE_INSENSITIVE);

  // query the java driver makes when refreshing node (i.e. after it comes back up)
  private static final Pattern queryPeerWithNamedParam =
      Pattern.compile(
          "\\s*SELECT\\s+\\*\\s+FROM\\s+system\\.peers\\s+WHERE\\s+peer\\s*=\\s*:address\\s*;?\\s*",
          Pattern.CASE_INSENSITIVE);
  private static final Pattern queryPeerV2WithNamedParam =
      Pattern.compile(
          "\\s*SELECT\\s+\\*\\s+FROM\\s+system\\.peers_v2\\s+WHERE\\s+peer\\s*=\\s*:address\\s+AND\\s+peer_port\\s*=\\s*:port\\s*;?\\s*",
          Pattern.CASE_INSENSITIVE);

  private final boolean supportsV2;

  public PeerMetadataHandler() {
    this(false);
  }

  public PeerMetadataHandler(boolean supportsV2) {
    this.supportsV2 = supportsV2;
    queryPatterns.add(queryClusterName);
    queryPatterns.add(queryPeerWithNamedParam);
    queryPatterns.add(queryPeers);
    queryPatterns.add(queryLocal);
    queryPatterns.add(queryPeersWithAddr);

    if (supportsV2) {
      queryPatterns.add(queryPeerV2WithNamedParam);
    }
  }

  @Override
  public boolean matches(Frame frame) {
    if (frame.message instanceof Query) {
      Query query = (Query) frame.message;
      String queryStr = query.query;
      for (Pattern pattern : queryPatterns) {
        if (pattern.matcher(queryStr).matches()) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public List<Action> getActions(AbstractNode node, Frame frame) {
    if (frame.message instanceof Query) {
      CqlMapper mapper = CqlMapper.forVersion(frame.protocolVersion);
      Query query = (Query) frame.message;

      Matcher clusterNameMatcher = queryClusterName.matcher(query.query);
      if (clusterNameMatcher.matches()) {
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
              },
              false);
        } else if (queryPeerWithNamedParam.matcher(query.query).matches()) {
          ByteBuffer addressBuffer = query.options.namedValues.get("address");
          InetAddress address = mapper.inet.decode(addressBuffer);
          return handlePeersQuery(node, mapper, n -> n.inet().equals(address), false);
        } else if (queryPeerV2WithNamedParam.matcher(query.query).matches()) {
          if (!supportsV2) {
            return peersV2NotSupported();
          }
          ByteBuffer addressBuffer = query.options.namedValues.get("address");
          InetAddress address = mapper.inet.decode(addressBuffer);
          ByteBuffer portBuffer = query.options.namedValues.get("port");
          int port = mapper.cint.decode(portBuffer);
          InetSocketAddress socketAddr = new InetSocketAddress(address, port);
          return handlePeersQuery(
              node, mapper, n -> n.inetSocketAddress().equals(socketAddr), true);
        }
        Matcher matcher = queryLocal.matcher(query.query);
        if (matcher.matches()) {
          return handleSystemLocalQuery(node, mapper);
        }
        matcher = queryPeers.matcher(query.query);
        if (matcher.matches()) {
          if (matcher.group(2).endsWith("v2")) {
            if (supportsV2) {
              return handlePeersQuery(node, mapper, n -> n != node, true);
            } else {
              return peersV2NotSupported();
            }
          }
          return handlePeersQuery(node, mapper, n -> n != node, false);
        }
      }
    }
    return Collections.emptyList();
  }

  private List<Action> peersV2NotSupported() {
    // Throw error when query made to peers_v2 table and v2 is not supported.
    return Collections.singletonList(
        new MessageResponseAction(new Error(INVALID, "Table system.peers_v2 does not exist")));
  }

  private Set<String> resolveTokens(AbstractNode node) {
    String[] t = node.resolvePeerInfo("tokens", "0").split(",");
    return new LinkedHashSet<>(Arrays.asList(t));
  }

  private List<Action> handleSystemLocalQuery(AbstractNode node, CqlMapper mapper) {
    InetSocketAddress address = resolveAddress(node);
    Codec<Set<String>> tokenCodec =
        mapper.codecFor(new RawType.RawSet(CodecUtils.primitive(ASCII)));

    List<ByteBuffer> localRow =
        CodecUtils.row(
            CodecUtils.encodePeerInfo(node, mapper.ascii::encode, "key", "local"),
            CodecUtils.encodePeerInfo(node, mapper.ascii::encode, "bootstrapped", "COMPLETED"),
            mapper.inet.encode(node.resolvePeerInfo("rpc_address", address.getAddress())),
            mapper.cint.encode(node.resolvePeerInfo("rpc_port", address.getPort())),
            mapper.inet.encode(node.resolvePeerInfo("broadcast_address", address.getAddress())),
            mapper.cint.encode(node.resolvePeerInfo("broadcast_port", address.getPort())),
            mapper.ascii.encode(node.getCluster().getName()),
            CodecUtils.encodePeerInfo(node, mapper.ascii::encode, "cql_version", "3.2.0"),
            mapper.ascii.encode(node.getDataCenter().getName()),
            mapper.inet.encode(node.resolvePeerInfo("listen_address", address.getAddress())),
            mapper.cint.encode(node.resolvePeerInfo("listen_port", address.getPort())),
            CodecUtils.encodePeerInfo(
                node,
                mapper.ascii::encode,
                "partitioner",
                "org.apache.cassandra.dht.Murmur3Partitioner"),
            CodecUtils.encodePeerInfo(node, mapper.ascii::encode, "rack", "rack1"),
            mapper.ascii.encode(node.resolveCassandraVersion()),
            tokenCodec.encode(resolveTokens(node)),
            mapper.uuid.encode(node.getHostId()),
            mapper.uuid.encode(schemaVersion));

    if (node.resolveDSEVersion() != null) {
      localRow.add(mapper.ascii.encode(node.resolveDSEVersion()));
      localRow.add(CodecUtils.encodePeerInfo(node, mapper.bool::encode, "graph", false));
    }

    Rows rows = new DefaultRows(buildSystemLocalRowsMetadata(node), CodecUtils.rows(localRow));
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  private List<Action> handleClusterNameQuery(AbstractNode node, CqlMapper mapper) {
    Queue<List<ByteBuffer>> clusterRow =
        CodecUtils.singletonRow(mapper.ascii.encode(node.getCluster().getName()));
    Rows rows = new DefaultRows(queryClusterNameMetadata, clusterRow);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  @SuppressWarnings("unchecked")
  private List<Action> handlePeersQuery(
      AbstractNode node, CqlMapper mapper, Predicate<AbstractNode> nodeFilter, boolean isV2) {
    // For each node matching the filter, provide its peer information.
    Codec<Set<String>> tokenCodec =
        mapper.codecFor(new RawType.RawSet(CodecUtils.primitive(ASCII)));

    AbstractCluster cluster = node.getCluster();
    Stream<AbstractNode> stream = cluster.getNodes().stream();

    Queue<List<ByteBuffer>> peerRows =
        stream
            .filter(nodeFilter)
            .map(
                n -> {
                  InetSocketAddress address = resolveAddress(n);

                  List<ByteBuffer> row =
                      CodecUtils.row(
                          mapper.inet.encode(n.resolvePeerInfo("peer", address.getAddress())),
                          mapper.varchar.encode(
                              n.resolvePeerInfo("data_center", n.getDataCenter().getName())),
                          CodecUtils.encodePeerInfo(n, mapper.varchar::encode, "rack", "rack1"),
                          mapper.varchar.encode(
                              n.resolvePeerInfo("release_version", n.resolveCassandraVersion())),
                          tokenCodec.encode(resolveTokens(n)),
                          mapper.uuid.encode(n.getHostId()),
                          mapper.uuid.encode(n.resolvePeerInfo("schema_version", schemaVersion)));

                  if (isV2) {
                    row.addAll(
                        CodecUtils.row(
                            mapper.cint.encode(n.resolvePeerInfo("peer_port", address.getPort())),
                            mapper.inet.encode(
                                n.resolvePeerInfo("native_address", address.getAddress())),
                            mapper.cint.encode(
                                n.resolvePeerInfo("native_port", address.getPort()))));
                  } else {
                    row.addAll(
                        CodecUtils.row(
                            mapper.inet.encode(
                                n.resolvePeerInfo("rpc_address", address.getAddress()))));
                  }
                  if (node.resolveDSEVersion() != null) {
                    row.add(mapper.ascii.encode(n.resolveDSEVersion()));
                    row.add(CodecUtils.encodePeerInfo(n, mapper.bool::encode, "graph", false));
                  }
                  return row;
                })
            .collect(Collectors.toCollection(ArrayDeque::new));

    Rows rows = new DefaultRows(buildSystemPeersRowsMetadata(node, isV2), peerRows);
    MessageResponseAction action = new MessageResponseAction(rows);
    return Collections.singletonList(action);
  }

  private InetSocketAddress resolveAddress(AbstractNode node) {
    InetSocketAddress address;
    if (node.getAddress() instanceof InetSocketAddress) {
      address = ((InetSocketAddress) node.getAddress());
    } else {
      address = new InetSocketAddress(InetAddress.getLoopbackAddress(), 9042);
    }
    return address;
  }

  private RowsMetadata buildSystemPeersRowsMetadata(AbstractNode node, boolean isV2) {
    CodecUtils.ColumnSpecBuilder systemPeers = CodecUtils.columnSpecBuilder("system", "peers");
    List<ColumnSpec> systemPeersSpecs =
        CodecUtils.columnSpecs(
            systemPeers.apply("peer", CodecUtils.primitive(INET)),
            systemPeers.apply("data_center", CodecUtils.primitive(ASCII)),
            systemPeers.apply("rack", CodecUtils.primitive(ASCII)),
            systemPeers.apply("release_version", CodecUtils.primitive(ASCII)),
            systemPeers.apply("tokens", new RawType.RawSet(CodecUtils.primitive(ASCII))),
            systemPeers.apply("host_id", CodecUtils.primitive(UUID)),
            systemPeers.apply("schema_version", CodecUtils.primitive(UUID)));

    if (isV2) {
      systemPeersSpecs.addAll(
          CodecUtils.columnSpecs(
              systemPeers.apply("peer_port", CodecUtils.primitive(INT)),
              systemPeers.apply("native_address", CodecUtils.primitive(INET)),
              systemPeers.apply("native_port", CodecUtils.primitive(INT))));
    } else {
      systemPeersSpecs.addAll(
          CodecUtils.columnSpecs(systemPeers.apply("rpc_address", CodecUtils.primitive(INET))));
    }

    if (node.resolveDSEVersion() != null) {
      systemPeersSpecs.add(systemPeers.apply("dse_version", CodecUtils.primitive(ASCII)));
      systemPeersSpecs.add(systemPeers.apply("graph", CodecUtils.primitive(BOOLEAN)));
    }

    int[] primaryKey = isV2 ? new int[] {0, 1} : new int[] {0};
    return new RowsMetadata(systemPeersSpecs, null, primaryKey, null);
  }

  private RowsMetadata buildSystemLocalRowsMetadata(AbstractNode node) {
    CodecUtils.ColumnSpecBuilder systemLocal = CodecUtils.columnSpecBuilder("system", "local");
    List<ColumnSpec> systemLocalSpecs =
        CodecUtils.columnSpecs(
            systemLocal.apply("key", CodecUtils.primitive(ASCII)),
            systemLocal.apply("bootstrapped", CodecUtils.primitive(ASCII)),
            systemLocal.apply("rpc_address", CodecUtils.primitive(INET)),
            systemLocal.apply("rpc_port", CodecUtils.primitive(INT)),
            systemLocal.apply("broadcast_address", CodecUtils.primitive(INET)),
            systemLocal.apply("broadcast_port", CodecUtils.primitive(INT)),
            systemLocal.apply("cluster_name", CodecUtils.primitive(ASCII)),
            systemLocal.apply("cql_version", CodecUtils.primitive(ASCII)),
            systemLocal.apply("data_center", CodecUtils.primitive(ASCII)),
            systemLocal.apply("listen_address", CodecUtils.primitive(INET)),
            systemLocal.apply("listen_port", CodecUtils.primitive(INT)),
            systemLocal.apply("partitioner", CodecUtils.primitive(ASCII)),
            systemLocal.apply("rack", CodecUtils.primitive(ASCII)),
            systemLocal.apply("release_version", CodecUtils.primitive(ASCII)),
            systemLocal.apply("tokens", new RawType.RawSet(CodecUtils.primitive(ASCII))),
            systemLocal.apply("host_id", CodecUtils.primitive(UUID)),
            systemLocal.apply("schema_version", CodecUtils.primitive(UUID)));
    if (node.resolveDSEVersion() != null) {
      systemLocalSpecs.add(systemLocal.apply("dse_version", CodecUtils.primitive(ASCII)));
      systemLocalSpecs.add(systemLocal.apply("graph", CodecUtils.primitive(BOOLEAN)));
    }
    return new RowsMetadata(systemLocalSpecs, null, new int[] {0}, null);
  }
}
