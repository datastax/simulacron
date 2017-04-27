package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RawType.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.*;
import static com.datastax.simulacron.common.codec.CodecUtils.getTypeFromName;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class CodecUtilsTest {

  @Test
  public void testGetTypeFromNamePrimitives() {
    assertThat(getTypeFromName("ascii")).isEqualTo(primitive(ASCII));
    assertThat(getTypeFromName("bigint")).isEqualTo(primitive(BIGINT));
    assertThat(getTypeFromName("blob")).isEqualTo(primitive(BLOB));
    assertThat(getTypeFromName("boolean")).isEqualTo(primitive(BOOLEAN));
    assertThat(getTypeFromName("timestamp")).isEqualTo(primitive(TIMESTAMP));
    assertThat(getTypeFromName("uuid")).isEqualTo(primitive(UUID));
    assertThat(getTypeFromName("varchar")).isEqualTo(primitive(VARCHAR));
    assertThat(getTypeFromName("varint")).isEqualTo(primitive(VARINT));
    assertThat(getTypeFromName("timeuuid")).isEqualTo(primitive(TIMEUUID));
    assertThat(getTypeFromName("inet")).isEqualTo(primitive(INET));
    assertThat(getTypeFromName("date")).isEqualTo(primitive(DATE));
    assertThat(getTypeFromName("time")).isEqualTo(primitive(TIME));
    assertThat(getTypeFromName("smallint")).isEqualTo(primitive(SMALLINT));
    assertThat(getTypeFromName("tinyint")).isEqualTo(primitive(TINYINT));
    assertThat(getTypeFromName("duration")).isEqualTo(primitive(DURATION));
  }

  @Test
  public void testGetTypeFromNameRepeatedCallsShouldReturnSameObject() {
    for (String name : new String[] {"ascii", "set<ascii>", "tuple<int,int,list<ascii>>"}) {
      // ensures the cache is working appropriately.
      RawType rawType = getTypeFromName(name);
      for (int i = 0; i < 10; i++) {
        assertThat(getTypeFromName(name)).isNotNull().isSameAs(rawType);
      }
    }
  }

  @Test
  public void testGetTypeFromNameShouldNotResolveNonPrimitiveNames() {
    String[] nonPrimitiveNames = {"custom", "list", "map", "set", "udt", "tuple"};
    for (String name : nonPrimitiveNames) {
      try {
        assertThat(getTypeFromName(name)).isNull();
        fail("InvalidTypeException expected for " + name);
      } catch (InvalidTypeException ite) {
        // expected
      }
    }
  }

  @Test
  public void testGetTypeFromNameList() {
    assertThat(getTypeFromName("list<int>")).isEqualTo(new RawList(primitive(INT)));
    assertThat(getTypeFromName("list<list<int>>"))
        .isEqualTo(new RawList(new RawList(primitive(INT))));
  }

  @Test
  public void testGetTypeFromNameSet() {
    assertThat(getTypeFromName("set<int>")).isEqualTo(new RawSet(primitive(INT)));
    assertThat(getTypeFromName("set<set<int>>")).isEqualTo(new RawSet(new RawSet(primitive(INT))));
  }

  @Test
  public void testGetTypeFromNameMap() {
    assertThat(getTypeFromName("map<int,ascii>"))
        .isEqualTo(new RawMap(primitive(INT), primitive(ASCII)));
    assertThat(getTypeFromName("map<int,map<ascii,tinyint>>"))
        .isEqualTo(new RawMap(primitive(INT), new RawMap(primitive(ASCII), primitive(TINYINT))));
  }

  @Test
  public void testGetTypeFromNameTuple() {
    List<RawType> elements = new ArrayList<>();
    elements.add(primitive(INT));
    elements.add(primitive(ASCII));
    elements.add(primitive(BLOB));
    assertThat(getTypeFromName("tuple<int,ascii,blob>")).isEqualTo(new RawTuple(elements));

    List<RawType> nestedElements = new ArrayList<>();
    nestedElements.add(primitive(INT));
    nestedElements.add(new RawTuple(elements));
    assertThat(getTypeFromName("tuple<int,tuple<int,ascii,blob>>"))
        .isEqualTo(new RawTuple(nestedElements));
  }

  @Test
  public void testGetTypeFromNameSpaces() {
    assertThat(getTypeFromName("map<int , ascii>"))
        .isEqualTo(new RawMap(primitive(INT), primitive(ASCII)));
  }

  @Test
  public void testGetTypeFromNameBadSyntax() {
    String[] badSyntax = {"map<string, int", "map<string>", "list<<>>", ""};
    for (String name : badSyntax) {
      try {
        assertThat(getTypeFromName(name)).isNull();
        fail("InvalidTypeException expected for " + name);
      } catch (InvalidTypeException ite) {
        // expected
      }
    }
  }

  @Test
  public void testGetTypeFromNameCustom() {
    assertThat(getTypeFromName("'IAMCustom'")).isEqualTo(new RawCustom("IAMCustom"));
    assertThat(getTypeFromName("empty")).isEqualTo(new RawCustom("empty"));
  }
}
