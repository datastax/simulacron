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
package com.datastax.oss.simulacron.protocol.json;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.protocol.internal.Frame;
import com.datastax.oss.protocol.internal.request.AuthResponse;
import com.datastax.oss.protocol.internal.request.Batch;
import com.datastax.oss.protocol.internal.request.Execute;
import com.datastax.oss.protocol.internal.request.Prepare;
import com.datastax.oss.protocol.internal.request.Query;
import com.datastax.oss.protocol.internal.request.Register;
import com.datastax.oss.protocol.internal.request.Startup;
import com.datastax.oss.protocol.internal.request.query.QueryOptions;
import com.datastax.oss.protocol.internal.response.Authenticate;
import com.datastax.oss.protocol.internal.response.Supported;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.assertj.core.util.Lists;
import org.assertj.core.util.Maps;
import org.junit.Test;

public class FrameSerializerTest {

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final ObjectWriter writer;
  private static final long time = 1513196542339L;

  static {
    mapper.registerModule(NativeProtocolModule.module());
    writer = mapper.writerWithDefaultPrettyPrinter();
  }

  @Test
  public void testSimpleFrameWarningsAndCustomPayload() throws Exception {
    UUID uuid = UUID.fromString("ce565629-e8e4-4f93-bdd4-9414b7d9d342");
    // 0x0708 == Bwg= base64 encoded
    Map<String, ByteBuffer> customPayload =
        Maps.newHashMap("custom", ByteBuffer.wrap(new byte[] {0x7, 0x8}));
    Frame frame =
        new Frame(
            5,
            true,
            10,
            true,
            uuid,
            customPayload,
            Lists.newArrayList("Something went wrong", "Fix me!"),
            com.datastax.oss.protocol.internal.request.Options.INSTANCE);

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 5,"
                + System.lineSeparator()
                + "  \"beta\" : true,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : \"ce565629-e8e4-4f93-bdd4-9414b7d9d342\","
                + System.lineSeparator()
                + "  \"custom_payload\" : {"
                + System.lineSeparator()
                + "    \"custom\" : \"Bwg=\""
                + System.lineSeparator()
                + "  },"
                + System.lineSeparator()
                + "  \"warnings\" : [ \"Something went wrong\", \"Fix me!\" ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"OPTIONS\","
                + System.lineSeparator()
                + "    \"opcode\" : 5,"
                + System.lineSeparator()
                + "    \"is_response\" : false"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testStartupFrame() throws Exception {
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Startup("LZ4"));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"STARTUP\","
                + System.lineSeparator()
                + "    \"opcode\" : 1,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"options\" : {"
                + System.lineSeparator()
                + "      \"CQL_VERSION\" : \"3.0.0\","
                + System.lineSeparator()
                + "      \"COMPRESSION\" : \"LZ4\""
                + System.lineSeparator()
                + "    }"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testAuthenticateFrame() throws Exception {
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Authenticate("AllowAllAuthenticator"));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"AUTHENTICATE\","
                + System.lineSeparator()
                + "    \"opcode\" : 3,"
                + System.lineSeparator()
                + "    \"is_response\" : true,"
                + System.lineSeparator()
                + "    \"authenticator\" : \"AllowAllAuthenticator\""
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testSupportedFrame() throws Exception {
    Map<String, List<String>> options = new LinkedHashMap<>();
    options.put("a", Lists.newArrayList("1", "2", "3"));
    options.put("b", Lists.newArrayList("hello"));
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Supported(options));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"SUPPORTED\","
                + System.lineSeparator()
                + "    \"opcode\" : 6,"
                + System.lineSeparator()
                + "    \"is_response\" : true,"
                + System.lineSeparator()
                + "    \"options\" : {"
                + System.lineSeparator()
                + "      \"a\" : [ \"1\", \"2\", \"3\" ],"
                + System.lineSeparator()
                + "      \"b\" : [ \"hello\" ]"
                + System.lineSeparator()
                + "    }"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testQueryPositionalValuesFrame() throws Exception {
    ByteBuffer value0 = ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x3}); // AQID base64 encoded
    ByteBuffer value1 = ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0xc}); // CgsM base64 encoded
    List<ByteBuffer> posValues = Lists.newArrayList(value0, value1);
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Query(
                "select * from base",
                new QueryOptions(
                    2, posValues, Collections.emptyMap(), false, 1000, null, 8, time, "myks")));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"QUERY\","
                + System.lineSeparator()
                + "    \"opcode\" : 7,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"query\" : \"select * from base\","
                + System.lineSeparator()
                + "    \"options\" : {"
                + System.lineSeparator()
                + "      \"consistency\" : \"TWO\","
                + System.lineSeparator()
                + "      \"positional_values\" : [ \"AQID\", \"CgsM\" ],"
                + System.lineSeparator()
                + "      \"named_values\" : { },"
                + System.lineSeparator()
                + "      \"skip_metadata\" : false,"
                + System.lineSeparator()
                + "      \"page_size\" : 1000,"
                + System.lineSeparator()
                + "      \"paging_state\" : null,"
                + System.lineSeparator()
                + "      \"serial_consistency\" : \"SERIAL\","
                + System.lineSeparator()
                + "      \"default_timestamp\" : 1513196542339,"
                + System.lineSeparator()
                + "      \"keyspace\" : \"myks\""
                + System.lineSeparator()
                + "    }"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testQueryNamedValuesFrame() throws Exception {
    ByteBuffer value0 = ByteBuffer.wrap(new byte[] {0x01, 0x02, 0x3}); // AQID base64 encoded
    ByteBuffer value1 = ByteBuffer.wrap(new byte[] {0x0a, 0x0b, 0xc}); // CgsM base64 encoded
    Map<String, ByteBuffer> namedValues = new LinkedHashMap<>();
    namedValues.put("a", value0);
    namedValues.put("b", value1);
    ByteBuffer pagingState =
        ByteBuffer.wrap(new byte[] {0xF, 0xD, 0x72, 0x7D, 0x52}); // Dw1yfVI= base64 encoded
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Query(
                "select * from base",
                new QueryOptions(
                    2,
                    Collections.emptyList(),
                    namedValues,
                    true,
                    1000,
                    pagingState,
                    8,
                    time,
                    "myks")));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"QUERY\","
                + System.lineSeparator()
                + "    \"opcode\" : 7,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"query\" : \"select * from base\","
                + System.lineSeparator()
                + "    \"options\" : {"
                + System.lineSeparator()
                + "      \"consistency\" : \"TWO\","
                + System.lineSeparator()
                + "      \"positional_values\" : [ ],"
                + System.lineSeparator()
                + "      \"named_values\" : {"
                + System.lineSeparator()
                + "        \"a\" : \"AQID\","
                + System.lineSeparator()
                + "        \"b\" : \"CgsM\""
                + System.lineSeparator()
                + "      },"
                + System.lineSeparator()
                + "      \"skip_metadata\" : true,"
                + System.lineSeparator()
                + "      \"page_size\" : 1000,"
                + System.lineSeparator()
                + "      \"paging_state\" : \"Dw1yfVI=\","
                + System.lineSeparator()
                + "      \"serial_consistency\" : \"SERIAL\","
                + System.lineSeparator()
                + "      \"default_timestamp\" : 1513196542339,"
                + System.lineSeparator()
                + "      \"keyspace\" : \"myks\""
                + System.lineSeparator()
                + "    }"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testPrepareFrame() throws Exception {
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Prepare("select * from base where belong=?", "your"));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"PREPARE\","
                + System.lineSeparator()
                + "    \"opcode\" : 9,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"query\" : \"select * from base where belong=?\","
                + System.lineSeparator()
                + "    \"keyspace\" : \"your\""
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testExecuteFrame() throws Exception {
    byte id[] = new byte[] {0x01, 0x02, 0x3, 0x04}; // AQIDBA== base64 encoded
    byte metadataId[] = new byte[] {0x4, 0x02, 0x3, 0x4}; // BAIDBA== base64 encoded
    ByteBuffer pagingState =
        ByteBuffer.wrap(new byte[] {0x08, 0x11, 0x1A, 0x1B}); // CBEaGW== base64 encoded
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Execute(
                id,
                metadataId,
                new QueryOptions(
                    2,
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    true,
                    1000,
                    pagingState,
                    8,
                    time,
                    "myks")));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"EXECUTE\","
                + System.lineSeparator()
                + "    \"opcode\" : 10,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"id\" : \"AQIDBA==\","
                + System.lineSeparator()
                + "    \"result_metadata_id\" : \"BAIDBA==\","
                + System.lineSeparator()
                + "    \"options\" : {"
                + System.lineSeparator()
                + "      \"consistency\" : \"TWO\","
                + System.lineSeparator()
                + "      \"positional_values\" : [ ],"
                + System.lineSeparator()
                + "      \"named_values\" : { },"
                + System.lineSeparator()
                + "      \"skip_metadata\" : true,"
                + System.lineSeparator()
                + "      \"page_size\" : 1000,"
                + System.lineSeparator()
                + "      \"paging_state\" : \"CBEaGw==\","
                + System.lineSeparator()
                + "      \"serial_consistency\" : \"SERIAL\","
                + System.lineSeparator()
                + "      \"default_timestamp\" : 1513196542339,"
                + System.lineSeparator()
                + "      \"keyspace\" : \"myks\""
                + System.lineSeparator()
                + "    }"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testRegisterFrame() throws Exception {
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Register(Lists.newArrayList("SCHEMA_CHANGE", "TOPOLOGY_CHANGE", "STATUS_CHANGE")));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"REGISTER\","
                + System.lineSeparator()
                + "    \"opcode\" : 11,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"event_types\" : [ \"SCHEMA_CHANGE\", \"TOPOLOGY_CHANGE\", \"STATUS_CHANGE\" ]"
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testBatchFrame() throws Exception {
    // 0x0807062A = CAcGKg== base64 encoded.
    List<Object> queriesOrIds =
        Lists.newArrayList(
            "select * from tbl", new byte[] {0x8, 0x7, 0x6, 0x2A}, "select * from world");
    // 0x00 == AA== base64 encoded
    List<List<ByteBuffer>> values =
        Lists.newArrayList(
            Lists.newArrayList(),
            Lists.newArrayList(ByteBuffer.wrap(new byte[] {0x00})),
            Lists.newArrayList());
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new Batch((byte) 1, queriesOrIds, values, 2, 10, time, "myks"));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"BATCH\","
                + System.lineSeparator()
                + "    \"opcode\" : 13,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"type\" : \"UNLOGGED\","
                + System.lineSeparator()
                + "    \"queries_or_ids\" : [ \"select * from tbl\", \"CAcGKg==\", \"select * from world\" ],"
                + System.lineSeparator()
                + "    \"values\" : [ [ ], [ \"AA==\" ], [ ] ],"
                + System.lineSeparator()
                + "    \"consistency\" : \"TWO\","
                + System.lineSeparator()
                + "    \"serial_consistency\" : \"LOCAL_ONE\","
                + System.lineSeparator()
                + "    \"default_timestamp\" : 1513196542339,"
                + System.lineSeparator()
                + "    \"keyspace\" : \"myks\""
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }

  @Test
  public void testAuthResponseFrame() throws Exception {
    Frame frame =
        new Frame(
            4,
            false,
            10,
            false,
            null,
            Collections.unmodifiableMap(new HashMap<>()),
            Collections.emptyList(),
            new AuthResponse(ByteBuffer.wrap(new byte[] {0x00})));

    String json = writer.writeValueAsString(frame);
    assertThat(json)
        .isEqualTo(
            "{"
                + System.lineSeparator()
                + "  \"protocol_version\" : 4,"
                + System.lineSeparator()
                + "  \"beta\" : false,"
                + System.lineSeparator()
                + "  \"stream_id\" : 10,"
                + System.lineSeparator()
                + "  \"tracing_id\" : null,"
                + System.lineSeparator()
                + "  \"custom_payload\" : { },"
                + System.lineSeparator()
                + "  \"warnings\" : [ ],"
                + System.lineSeparator()
                + "  \"message\" : {"
                + System.lineSeparator()
                + "    \"type\" : \"AUTHRESPONSE\","
                + System.lineSeparator()
                + "    \"opcode\" : 15,"
                + System.lineSeparator()
                + "    \"is_response\" : false,"
                + System.lineSeparator()
                + "    \"token\" : \"AA==\""
                + System.lineSeparator()
                + "  }"
                + System.lineSeparator()
                + "}");
  }
}
