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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
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
    OBJECT_MAPPER = om;
  }

  /**
   * Returns the {@link ObjectMapper} that knows how to serialize and deserialize {@link
   * ClusterSpec} and its members, as well as query prime requests.
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
