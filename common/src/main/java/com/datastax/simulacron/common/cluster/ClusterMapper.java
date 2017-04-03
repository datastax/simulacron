package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ClusterMapper {

  public static ObjectMapper getMapper() {
    ObjectMapper om = new ObjectMapper();

    SimpleModule mod = new SimpleModule("ClusterSerializers");
    // Custom serializer for socket address
    mod.addDeserializer(SocketAddress.class, new SocketAddressDeserializer());
    om.registerModule(mod);
    // Exclude null / emptys.
    om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    return om;
  }

  public static void main(String args[]) throws Exception {
    ObjectMapper om = getMapper();
    Cluster cluster =
        Cluster.builder()
            .withName("cluster1")
            .withCassandraVersion("1.2.19")
            .withPeerInfo("hello", "world")
            .withPeerInfo("goodbye", "sun")
            .withNodes(10, 10)
            .build();
    String json = om.writeValueAsString(cluster);
    System.out.println(json);

    Cluster cluster2 = om.readValue(json, Cluster.class);
    System.out.println(om.writeValueAsString(cluster2));
  }

  public static class SocketAddressDeserializer extends StdDeserializer<SocketAddress> {

    SocketAddressDeserializer() {
      super(SocketAddress.class);
    }

    @Override
    public SocketAddress deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      String data = p.getText();
      String[] parts = data.split(":");
      String addr = parts[0];
      int port = Integer.parseInt(parts[1]);
      // TODO: Handle local address
      return new InetSocketAddress(InetAddress.getByName(addr), port);
    }
  }
}
