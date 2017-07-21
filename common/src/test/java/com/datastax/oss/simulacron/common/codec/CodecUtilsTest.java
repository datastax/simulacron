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
package com.datastax.oss.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RawType.RawCustom;
import com.datastax.oss.protocol.internal.response.result.RawType.RawList;
import com.datastax.oss.protocol.internal.response.result.RawType.RawMap;
import com.datastax.oss.protocol.internal.response.result.RawType.RawSet;
import com.datastax.oss.protocol.internal.response.result.RawType.RawTuple;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DURATION;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INET;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.INT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.SMALLINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIME;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMESTAMP;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TIMEUUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.TINYINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.UUID;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CodecUtilsTest {

  @Test
  public void testGetTypeFromNamePrimitives() {
    assertThat(CodecUtils.getTypeFromName("ascii")).isEqualTo(CodecUtils.primitive(ASCII));
    assertThat(CodecUtils.getTypeFromName("bigint")).isEqualTo(CodecUtils.primitive(BIGINT));
    assertThat(CodecUtils.getTypeFromName("blob")).isEqualTo(CodecUtils.primitive(BLOB));
    assertThat(CodecUtils.getTypeFromName("boolean")).isEqualTo(CodecUtils.primitive(BOOLEAN));
    assertThat(CodecUtils.getTypeFromName("timestamp")).isEqualTo(CodecUtils.primitive(TIMESTAMP));
    assertThat(CodecUtils.getTypeFromName("uuid")).isEqualTo(CodecUtils.primitive(UUID));
    assertThat(CodecUtils.getTypeFromName("varchar")).isEqualTo(CodecUtils.primitive(VARCHAR));
    assertThat(CodecUtils.getTypeFromName("varint")).isEqualTo(CodecUtils.primitive(VARINT));
    assertThat(CodecUtils.getTypeFromName("timeuuid")).isEqualTo(CodecUtils.primitive(TIMEUUID));
    assertThat(CodecUtils.getTypeFromName("inet")).isEqualTo(CodecUtils.primitive(INET));
    assertThat(CodecUtils.getTypeFromName("date")).isEqualTo(CodecUtils.primitive(DATE));
    assertThat(CodecUtils.getTypeFromName("time")).isEqualTo(CodecUtils.primitive(TIME));
    assertThat(CodecUtils.getTypeFromName("smallint")).isEqualTo(CodecUtils.primitive(SMALLINT));
    assertThat(CodecUtils.getTypeFromName("tinyint")).isEqualTo(CodecUtils.primitive(TINYINT));
    assertThat(CodecUtils.getTypeFromName("duration")).isEqualTo(CodecUtils.primitive(DURATION));
  }

  @Test
  public void testGetTypeFromNameRepeatedCallsShouldReturnSameObject() {
    for (String name : new String[] {"ascii", "set<ascii>", "tuple<int,int,list<ascii>>"}) {
      // ensures the cache is working appropriately.
      RawType rawType = CodecUtils.getTypeFromName(name);
      for (int i = 0; i < 10; i++) {
        assertThat(CodecUtils.getTypeFromName(name)).isNotNull().isSameAs(rawType);
      }
    }
  }

  @Test
  public void testGetTypeFromNameShouldNotResolveNonPrimitiveNames() {
    String[] nonPrimitiveNames = {"custom", "list", "map", "set", "udt", "tuple"};
    for (String name : nonPrimitiveNames) {
      try {
        assertThat(CodecUtils.getTypeFromName(name)).isNull();
        fail("InvalidTypeException expected for " + name);
      } catch (InvalidTypeException ite) {
        // expected
      }
    }
  }

  @Test
  public void testGetTypeFromNameList() {
    assertThat(CodecUtils.getTypeFromName("list<int>"))
        .isEqualTo(new RawList(CodecUtils.primitive(INT)));
    assertThat(CodecUtils.getTypeFromName("list<list<int>>"))
        .isEqualTo(new RawList(new RawList(CodecUtils.primitive(INT))));
  }

  @Test
  public void testGetTypeFromNameSet() {
    assertThat(CodecUtils.getTypeFromName("set<int>"))
        .isEqualTo(new RawSet(CodecUtils.primitive(INT)));
    assertThat(CodecUtils.getTypeFromName("set<set<int>>"))
        .isEqualTo(new RawSet(new RawSet(CodecUtils.primitive(INT))));
  }

  @Test
  public void testGetTypeFromNameMap() {
    assertThat(CodecUtils.getTypeFromName("map<int,ascii>"))
        .isEqualTo(new RawMap(CodecUtils.primitive(INT), CodecUtils.primitive(ASCII)));
    assertThat(CodecUtils.getTypeFromName("map<int,map<ascii,tinyint>>"))
        .isEqualTo(
            new RawMap(
                CodecUtils.primitive(INT),
                new RawMap(CodecUtils.primitive(ASCII), CodecUtils.primitive(TINYINT))));
  }

  @Test
  public void testGetTypeFromNameTuple() {
    List<RawType> elements = new ArrayList<>();
    elements.add(CodecUtils.primitive(INT));
    elements.add(CodecUtils.primitive(ASCII));
    elements.add(CodecUtils.primitive(BLOB));
    assertThat(CodecUtils.getTypeFromName("tuple<int,ascii,blob>"))
        .isEqualTo(new RawTuple(elements));

    List<RawType> nestedElements = new ArrayList<>();
    nestedElements.add(CodecUtils.primitive(INT));
    nestedElements.add(new RawTuple(elements));
    assertThat(CodecUtils.getTypeFromName("tuple<int,tuple<int,ascii,blob>>"))
        .isEqualTo(new RawTuple(nestedElements));
  }

  @Test
  public void testGetTypeFromNameSpaces() {
    assertThat(CodecUtils.getTypeFromName("map<int , ascii>"))
        .isEqualTo(new RawMap(CodecUtils.primitive(INT), CodecUtils.primitive(ASCII)));
  }

  @Test
  public void testGetTypeFromNameBadSyntax() {
    String[] badSyntax = {"map<string, int", "map<string>", "list<<>>", ""};
    for (String name : badSyntax) {
      try {
        assertThat(CodecUtils.getTypeFromName(name)).isNull();
        fail("InvalidTypeException expected for " + name);
      } catch (InvalidTypeException ite) {
        // expected
      }
    }
  }

  @Test
  public void testGetTypeFromNameCustom() {
    assertThat(CodecUtils.getTypeFromName("'IAMCustom'")).isEqualTo(new RawCustom("IAMCustom"));
    assertThat(CodecUtils.getTypeFromName("empty")).isEqualTo(new RawCustom("empty"));
  }
}
