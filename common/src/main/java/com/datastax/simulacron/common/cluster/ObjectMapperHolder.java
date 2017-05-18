package com.datastax.simulacron.common.cluster;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;

public class ObjectMapperHolder {

  private static final ObjectMapper OBJECT_MAPPER;

  static {
    ObjectMapper om = new ObjectMapper();
    SimpleModule mod = new SimpleModule("ClusterSerializers");
    // Custom serializer for socket address
    mod.addDeserializer(SocketAddress.class, new SocketAddressDeserializer());
    mod.addKeySerializer(InetAddress.class, new InetAddressSerializer());
    mod.addKeyDeserializer(InetAddress.class, new InetAddressDeserializer());
    om.registerModule(mod);
    // Exclude null / emptys.
    om.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
    OBJECT_MAPPER = om;
  }

  /**
   * Returns the {@link ObjectMapper} that knows how to serialize and deserialize {@link Cluster}
   * and its members, as well as query prime requests.
   *
   * @return the {@link ObjectMapper}
   */
  public static ObjectMapper getMapper() {
    return OBJECT_MAPPER;
  }

  /**
   * A custom deserializer for deserializing an address in the format of X.X.X.X:YYYY into a {@link
   * SocketAddress}.
   *
   * <p>This does not currently work for Inet6 addresses presumably. It also does not work for
   * LocalAddress as well.
   */
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

  public static class InetAddressDeserializer extends KeyDeserializer {

    InetAddressDeserializer() {}

    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
      return InetAddress.getByName(key);
    }
  }

  public static class InetAddressSerializer extends JsonSerializer<InetAddress> {

    @Override
    public void serialize(InetAddress value, JsonGenerator gen, SerializerProvider serializers)
        throws IOException {
      // Adapted from com.fasterxml.jackson.databind.ser.std.InetAddressSerializer but writes field name instead
      // of string.
      // Ok: get textual description; choose "more specific" part
      String str = value.toString().trim();
      int ix = str.indexOf('/');
      if (ix >= 0) {
        if (ix == 0) { // missing host name; use address
          str = str.substring(1);
        } else { // otherwise use name
          str = str.substring(0, ix);
        }
      }
      gen.writeFieldName(str);
    }
  }
}
