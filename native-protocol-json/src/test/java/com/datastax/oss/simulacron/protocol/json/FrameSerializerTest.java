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
            "{\n"
                + "  \"protocol_version\" : 5,\n"
                + "  \"beta\" : true,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : \"ce565629-e8e4-4f93-bdd4-9414b7d9d342\",\n"
                + "  \"custom_payload\" : {\n"
                + "    \"custom\" : \"Bwg=\"\n"
                + "  },\n"
                + "  \"warnings\" : [ \"Something went wrong\", \"Fix me!\" ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"OPTIONS\",\n"
                + "    \"opcode\" : 5,\n"
                + "    \"is_response\" : false\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"STARTUP\",\n"
                + "    \"opcode\" : 1,\n"
                + "    \"is_response\" : false,\n"
                + "    \"options\" : {\n"
                + "      \"CQL_VERSION\" : \"3.0.0\",\n"
                + "      \"COMPRESSION\" : \"LZ4\"\n"
                + "    }\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"AUTHENTICATE\",\n"
                + "    \"opcode\" : 3,\n"
                + "    \"is_response\" : true,\n"
                + "    \"authenticator\" : \"AllowAllAuthenticator\"\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"SUPPORTED\",\n"
                + "    \"opcode\" : 6,\n"
                + "    \"is_response\" : true,\n"
                + "    \"options\" : {\n"
                + "      \"a\" : [ \"1\", \"2\", \"3\" ],\n"
                + "      \"b\" : [ \"hello\" ]\n"
                + "    }\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"QUERY\",\n"
                + "    \"opcode\" : 7,\n"
                + "    \"is_response\" : false,\n"
                + "    \"query\" : \"select * from base\",\n"
                + "    \"options\" : {\n"
                + "      \"consistency\" : \"TWO\",\n"
                + "      \"positional_values\" : [ \"AQID\", \"CgsM\" ],\n"
                + "      \"named_values\" : { },\n"
                + "      \"skip_metadata\" : false,\n"
                + "      \"page_size\" : 1000,\n"
                + "      \"paging_state\" : null,\n"
                + "      \"serial_consistency\" : \"SERIAL\",\n"
                + "      \"default_timestamp\" : 1513196542339,\n"
                + "      \"keyspace\" : \"myks\"\n"
                + "    }\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"QUERY\",\n"
                + "    \"opcode\" : 7,\n"
                + "    \"is_response\" : false,\n"
                + "    \"query\" : \"select * from base\",\n"
                + "    \"options\" : {\n"
                + "      \"consistency\" : \"TWO\",\n"
                + "      \"positional_values\" : [ ],\n"
                + "      \"named_values\" : {\n"
                + "        \"a\" : \"AQID\",\n"
                + "        \"b\" : \"CgsM\"\n"
                + "      },\n"
                + "      \"skip_metadata\" : true,\n"
                + "      \"page_size\" : 1000,\n"
                + "      \"paging_state\" : \"Dw1yfVI=\",\n"
                + "      \"serial_consistency\" : \"SERIAL\",\n"
                + "      \"default_timestamp\" : 1513196542339,\n"
                + "      \"keyspace\" : \"myks\"\n"
                + "    }\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"PREPARE\",\n"
                + "    \"opcode\" : 9,\n"
                + "    \"is_response\" : false,\n"
                + "    \"query\" : \"select * from base where belong=?\",\n"
                + "    \"keyspace\" : \"your\"\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"EXECUTE\",\n"
                + "    \"opcode\" : 10,\n"
                + "    \"is_response\" : false,\n"
                + "    \"id\" : \"AQIDBA==\",\n"
                + "    \"result_metadata_id\" : \"BAIDBA==\",\n"
                + "    \"options\" : {\n"
                + "      \"consistency\" : \"TWO\",\n"
                + "      \"positional_values\" : [ ],\n"
                + "      \"named_values\" : { },\n"
                + "      \"skip_metadata\" : true,\n"
                + "      \"page_size\" : 1000,\n"
                + "      \"paging_state\" : \"CBEaGw==\",\n"
                + "      \"serial_consistency\" : \"SERIAL\",\n"
                + "      \"default_timestamp\" : 1513196542339,\n"
                + "      \"keyspace\" : \"myks\"\n"
                + "    }\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"REGISTER\",\n"
                + "    \"opcode\" : 11,\n"
                + "    \"is_response\" : false,\n"
                + "    \"event_types\" : [ \"SCHEMA_CHANGE\", \"TOPOLOGY_CHANGE\", \"STATUS_CHANGE\" ]\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"BATCH\",\n"
                + "    \"opcode\" : 13,\n"
                + "    \"is_response\" : false,\n"
                + "    \"type\" : \"UNLOGGED\",\n"
                + "    \"queries_or_ids\" : [ \"select * from tbl\", \"CAcGKg==\", \"select * from world\" ],\n"
                + "    \"values\" : [ [ ], [ \"AA==\" ], [ ] ],\n"
                + "    \"consistency\" : \"TWO\",\n"
                + "    \"serial_consistency\" : \"LOCAL_ONE\",\n"
                + "    \"default_timestamp\" : 1513196542339,\n"
                + "    \"keyspace\" : \"myks\"\n"
                + "  }\n"
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
            "{\n"
                + "  \"protocol_version\" : 4,\n"
                + "  \"beta\" : false,\n"
                + "  \"stream_id\" : 10,\n"
                + "  \"tracing_id\" : null,\n"
                + "  \"custom_payload\" : { },\n"
                + "  \"warnings\" : [ ],\n"
                + "  \"message\" : {\n"
                + "    \"type\" : \"AUTHRESPONSE\",\n"
                + "    \"opcode\" : 15,\n"
                + "    \"is_response\" : false,\n"
                + "    \"token\" : \"AA==\"\n"
                + "  }\n"
                + "}");
  }
}
