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

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BIGINT;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BLOB;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.BOOLEAN;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DATE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DECIMAL;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.DOUBLE;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.FLOAT;
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
import static com.datastax.oss.simulacron.common.Assertions.assertThat;
import static com.datastax.oss.simulacron.common.codec.CodecUtils.primitive;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RawType.RawList;
import com.datastax.oss.protocol.internal.response.result.RawType.RawSet;
import com.datastax.oss.protocol.internal.response.result.RawType.RawTuple;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import org.junit.Test;

public class CqlMapperTest {

  private CqlMapper mapper = CqlMapper.forVersion(4);

  @Test
  public void shouldHandleAscii() {
    Codec<String> asciiCodec = mapper.codecFor(primitive(ASCII));
    assertThat(asciiCodec).isEqualTo(mapper.ascii);

    encodeAndDecode(asciiCodec, "hello", "0x68656c6c6f");
    encodeAndDecode(asciiCodec, "", "0x");
    encodeAndDecode(asciiCodec, null, null);
  }

  @Test
  public void shouldHandleBigint() {
    Codec<Long> bigintCodec = mapper.codecFor(primitive(BIGINT));
    assertThat(bigintCodec).isSameAs(mapper.bigint);

    encodeAndDecode(bigintCodec, 0L, "0x0000000000000000");
    encodeAndDecode(bigintCodec, 1L, "0x0000000000000001");
    encodeAndDecode(bigintCodec, Long.MIN_VALUE, "0x8000000000000000");
    encodeAndDecode(bigintCodec, Long.MAX_VALUE, "0x7fffffffffffffff");
    encodeAndDecode(bigintCodec, null, null, 0L);

    encodeObjectAndDecode(bigintCodec, null, null, 0L);
    encodeObjectAndDecode(bigintCodec, 7L, "0x0000000000000007", 7L);
    encodeObjectAndDecode(bigintCodec, 7, "0x0000000000000007", 7L);
    encodeObjectAndDecode(bigintCodec, "7", "0x0000000000000007", 7L);
    encodeObjectAndDecode(bigintCodec, Optional.of(12), null, 0L);

    try {
      // not enough bytes.
      bigintCodec.decode(Bytes.fromHexString("0x00ff"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleBlob() {
    Codec<ByteBuffer> blobCodec = mapper.codecFor(primitive(BLOB));
    assertThat(blobCodec).isSameAs(mapper.blob);

    encodeAndDecode(blobCodec, Bytes.fromHexString("0x"), "0x");
    encodeAndDecode(blobCodec, Bytes.fromHexString("0x742f7e"), "0x742f7e");
    encodeAndDecode(blobCodec, null, null);

    ByteBuffer buf = Bytes.fromHexString("0x8679FE");
    encodeObjectAndDecode(blobCodec, null, null, null);
    encodeObjectAndDecode(blobCodec, "0x8679FE", "0x8679fe", buf);
    encodeObjectAndDecode(blobCodec, buf, "0x8679fe", buf);
    encodeObjectAndDecode(blobCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleBoolean() {
    Codec<Boolean> booleanCodec = mapper.codecFor(primitive(BOOLEAN));
    assertThat(booleanCodec).isSameAs(mapper.bool);

    encodeAndDecode(booleanCodec, true, "0x01");
    encodeAndDecode(booleanCodec, false, "0x00");
    encodeAndDecode(booleanCodec, null, null, false);

    encodeObjectAndDecode(booleanCodec, false, "0x00", false);
    encodeObjectAndDecode(booleanCodec, "TRUE", "0x01", true);
    encodeObjectAndDecode(booleanCodec, 2, "0x01", true);
    encodeObjectAndDecode(booleanCodec, null, null, false);
    encodeObjectAndDecode(booleanCodec, Optional.of(12), null, false);

    try {
      // too many bytes
      booleanCodec.decode(Bytes.fromHexString("0x0100"));
    } catch (InvalidTypeException e) {
      // expected
    }

    // empty bytebuf should yield false.
    assertThat(booleanCodec.decode(Bytes.fromHexString("0x"))).isEqualTo(false);
  }

  @Test
  public void shouldHandleDate() {
    Codec<LocalDate> dateCodec = mapper.codecFor(primitive(DATE));
    assertThat(dateCodec).isSameAs(mapper.date);

    encodeAndDecode(dateCodec, LocalDate.of(2017, 7, 4), "0x800043c7");
    encodeAndDecode(dateCodec, LocalDate.of(1970, 1, 1), "0x80000000");
    encodeAndDecode(dateCodec, LocalDate.of(1969, 12, 31), "0x7fffffff");
    encodeAndDecode(dateCodec, null, null);

    encodeObjectAndDecode(
        dateCodec, LocalDate.of(2017, 7, 4), "0x800043c7", LocalDate.of(2017, 7, 4));
    encodeObjectAndDecode(dateCodec, 0, "0x80000000", LocalDate.of(1970, 1, 1));
    encodeObjectAndDecode(dateCodec, null, null, null);
    encodeObjectAndDecode(dateCodec, "2018-05-03", "0x800044f6", LocalDate.parse("2018-05-03"));
    encodeObjectAndDecode(dateCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleDecimal() {
    Codec<BigDecimal> decimalCodec = mapper.codecFor(primitive(DECIMAL));
    assertThat(decimalCodec).isSameAs(mapper.decimal);

    encodeAndDecode(
        decimalCodec, new BigDecimal(8675e39), "0x00000000639588a2c184700000000000000000000000");
    encodeAndDecode(decimalCodec, new BigDecimal(0), "0x0000000000");
    encodeAndDecode(decimalCodec, null, null);

    encodeObjectAndDecode(
        decimalCodec,
        new BigDecimal(8675e39),
        "0x00000000639588a2c184700000000000000000000000",
        new BigDecimal(8675e39));

    encodeObjectAndDecode(decimalCodec, null, null, null);
    encodeObjectAndDecode(decimalCodec, 5, "0x0000000005", new BigDecimal(5));
    encodeObjectAndDecode(decimalCodec, "5", "0x0000000005", new BigDecimal(5));
    encodeObjectAndDecode(decimalCodec, Optional.of(12), null, null);

    try {
      // not enough bytes.
      decimalCodec.decode(Bytes.fromHexString("0x00ffef"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleDouble() {
    Codec<Double> doubleCodec = mapper.codecFor(primitive(DOUBLE));
    assertThat(doubleCodec).isSameAs(mapper.cdouble);

    encodeAndDecode(doubleCodec, 0.0, "0x0000000000000000");
    encodeAndDecode(doubleCodec, 86.765309, "0x4055b0fad2999568");
    encodeAndDecode(doubleCodec, Double.MIN_VALUE, "0x0000000000000001");
    encodeAndDecode(doubleCodec, Double.MAX_VALUE, "0x7fefffffffffffff");
    encodeAndDecode(doubleCodec, null, null, 0.0);

    encodeObjectAndDecode(doubleCodec, 86.765309, "0x4055b0fad2999568", 86.765309);
    // precision loss
    encodeObjectAndDecode(doubleCodec, 86.765309f, "0x4055b0fae0000000", 86.76531219482422);
    encodeObjectAndDecode(doubleCodec, "86.765309", "0x4055b0fad2999568", 86.765309);
    encodeObjectAndDecode(doubleCodec, null, null, 0.0);
    encodeObjectAndDecode(doubleCodec, Optional.of(12), null, 0.0);

    try {
      // not enough bytes.
      doubleCodec.decode(Bytes.fromHexString("0x00ffef"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleFloat() {
    Codec<Float> floatCodec = mapper.codecFor(primitive(FLOAT));
    assertThat(floatCodec).isSameAs(mapper.cfloat);

    encodeAndDecode(floatCodec, 0.0f, "0x00000000");
    encodeAndDecode(floatCodec, 86.765309f, "0x42ad87d7");
    encodeAndDecode(floatCodec, Float.MIN_VALUE, "0x00000001");
    encodeAndDecode(floatCodec, Float.MAX_VALUE, "0x7f7fffff");
    encodeAndDecode(floatCodec, null, null, 0.0f);

    encodeObjectAndDecode(floatCodec, 86.765309, "0x42ad87d7", 86.765309f);
    encodeObjectAndDecode(floatCodec, 86.765309f, "0x42ad87d7", 86.765309f);
    encodeObjectAndDecode(floatCodec, "86.765309", "0x42ad87d7", 86.765309f);
    encodeObjectAndDecode(floatCodec, null, null, 0.0f);
    encodeObjectAndDecode(floatCodec, Optional.of(12), null, 0.0f);

    try {
      // not enough bytes.
      floatCodec.decode(Bytes.fromHexString("0x00ff"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleInet() throws Exception {
    Codec<InetAddress> inetCodec = mapper.codecFor(primitive(INET));
    assertThat(inetCodec).isSameAs(mapper.inet);

    InetAddress ipv6 =
        InetAddress.getByAddress(
            new byte[] {127, 0, 0, 1, 127, 0, 0, 2, 127, 0, 0, 3, 122, 127, 125, 124});

    InetAddress ipv4 = InetAddress.getByAddress(new byte[] {127, 0, 0, 1});

    // ipv4
    encodeAndDecode(inetCodec, ipv4, "0x7f000001");
    // ipv6
    encodeAndDecode(inetCodec, ipv6, "0x7f0000017f0000027f0000037a7f7d7c");
    encodeAndDecode(inetCodec, null, null);

    encodeObjectAndDecode(inetCodec, ipv4, "0x7f000001", ipv4);
    encodeObjectAndDecode(inetCodec, "127.0.0.1", "0x7f000001", ipv4);
    encodeObjectAndDecode(inetCodec, "notanip.>>>", null, null);
    encodeObjectAndDecode(inetCodec, null, null, null);
    encodeObjectAndDecode(inetCodec, Optional.of(12), null, null);

    try {
      // invalid number of bytes.
      inetCodec.decode(Bytes.fromHexString("0x7f00000102"));
      fail("Shouldn't have been able to decode 5 byte address");
    } catch (InvalidTypeException e) {
      assertThat(e.getCause()).isInstanceOf(UnknownHostException.class);
    }
  }

  @Test
  public void shouldHandleInt() throws Exception {
    Codec<Integer> intCodec = mapper.codecFor(primitive(INT));
    assertThat(intCodec).isSameAs(mapper.cint);

    encodeAndDecode(intCodec, 0, "0x00000000");
    encodeAndDecode(intCodec, Integer.MAX_VALUE, "0x7fffffff");
    encodeAndDecode(intCodec, Integer.MIN_VALUE, "0x80000000");
    encodeAndDecode(intCodec, null, null, 0);

    encodeObjectAndDecode(intCodec, 1, "0x00000001", 1);
    encodeObjectAndDecode(intCodec, 1.0, "0x00000001", 1);
    encodeObjectAndDecode(intCodec, "1", "0x00000001", 1);
    encodeObjectAndDecode(intCodec, null, null, 0);
    encodeObjectAndDecode(intCodec, Optional.of(12), null, 0);

    try {
      // not enough bytes.
      intCodec.decode(Bytes.fromHexString("0x00ff"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleSmallint() throws Exception {
    Codec<Short> smallintCodec = mapper.codecFor(primitive(SMALLINT));
    assertThat(smallintCodec).isSameAs(mapper.smallint);

    encodeAndDecode(smallintCodec, (short) 0, "0x0000");
    encodeAndDecode(smallintCodec, Short.MAX_VALUE, "0x7fff");
    encodeAndDecode(smallintCodec, Short.MIN_VALUE, "0x8000");
    encodeAndDecode(smallintCodec, null, null, (short) 0);

    encodeObjectAndDecode(smallintCodec, (short) 1, "0x0001", (short) 1);
    encodeObjectAndDecode(smallintCodec, 1.0, "0x0001", (short) 1);
    encodeObjectAndDecode(smallintCodec, "1", "0x0001", (short) 1);
    encodeObjectAndDecode(smallintCodec, null, null, (short) 0);
    encodeObjectAndDecode(smallintCodec, Optional.of(12), null, (short) 0.0);

    try {
      // not enough bytes.
      smallintCodec.decode(Bytes.fromHexString("0x00"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleTime() throws Exception {
    Codec<Long> timeCodec = mapper.codecFor(primitive(TIME));
    assertThat(timeCodec).isSameAs(mapper.time);

    encodeAndDecode(timeCodec, 0L, "0x0000000000000000");
    encodeAndDecode(timeCodec, 1L, "0x0000000000000001");
    encodeAndDecode(timeCodec, Long.MIN_VALUE, "0x8000000000000000");
    encodeAndDecode(timeCodec, Long.MAX_VALUE, "0x7fffffffffffffff");
    encodeAndDecode(timeCodec, null, null, 0L);

    encodeObjectAndDecode(timeCodec, null, null, 0L);
    encodeObjectAndDecode(timeCodec, 7L, "0x0000000000000007", 7L);
    encodeObjectAndDecode(timeCodec, 7, "0x0000000000000007", 7L);
    encodeObjectAndDecode(timeCodec, "7", "0x0000000000000007", 7L);
    encodeObjectAndDecode(timeCodec, Optional.of(12), null, 0L);
  }

  @Test
  public void shouldHandleTimestamp() throws Exception {
    Codec<Date> timestampCodec = mapper.codecFor(primitive(TIMESTAMP));
    assertThat(timestampCodec).isSameAs(mapper.timestamp);

    encodeAndDecode(timestampCodec, new Date(0), "0x0000000000000000");
    encodeAndDecode(timestampCodec, null, null);

    encodeObjectAndDecode(timestampCodec, null, null, null);
    encodeObjectAndDecode(timestampCodec, 7L, "0x0000000000000007", new Date(7));
    encodeObjectAndDecode(timestampCodec, "7", "0x0000000000000007", new Date(7));
    encodeObjectAndDecode(timestampCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleTimeuuid() throws Exception {
    Codec<UUID> timeuuidCodec = mapper.codecFor(primitive(TIMEUUID));
    assertThat(timeuuidCodec).isSameAs(mapper.timeuuid);

    String uuidStr = "5bc64000-2157-11e7-bf6d-934d002a66ff";
    UUID uuid = java.util.UUID.fromString(uuidStr);
    String byteStr = "0x5bc64000215711e7bf6d934d002a66ff";
    encodeAndDecode(timeuuidCodec, uuid, byteStr);
    encodeAndDecode(timeuuidCodec, null, null);

    encodeObjectAndDecode(timeuuidCodec, uuidStr, byteStr, uuid);
    encodeObjectAndDecode(timeuuidCodec, uuid, byteStr, uuid);
    encodeObjectAndDecode(timeuuidCodec, null, null, null);
    encodeObjectAndDecode(timeuuidCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleTinyint() throws Exception {
    Codec<Byte> tinyintCodec = mapper.codecFor(primitive(TINYINT));
    assertThat(tinyintCodec).isSameAs(mapper.tinyint);

    encodeAndDecode(tinyintCodec, (byte) 0, "0x00");
    encodeAndDecode(tinyintCodec, Byte.MAX_VALUE, "0x7f");
    encodeAndDecode(tinyintCodec, Byte.MIN_VALUE, "0x80");
    encodeAndDecode(tinyintCodec, null, null, (byte) 0);

    encodeObjectAndDecode(tinyintCodec, (byte) 1, "0x01", (byte) 1);
    encodeObjectAndDecode(tinyintCodec, 1.0, "0x01", (byte) 1);
    encodeObjectAndDecode(tinyintCodec, "1", "0x01", (byte) 1);
    encodeObjectAndDecode(tinyintCodec, null, null, (byte) 0);
    encodeObjectAndDecode(tinyintCodec, Optional.of(12), null, (byte) 0);

    try {
      // too many bytes
      tinyintCodec.decode(Bytes.fromHexString("0x0404"));
    } catch (InvalidTypeException e) {
      // expected
    }
  }

  @Test
  public void shouldHandleUuid() throws Exception {
    Codec<UUID> uuidCodec = mapper.codecFor(primitive(UUID));
    assertThat(uuidCodec).isSameAs(mapper.uuid);

    String uuidStr = "d79c8651-b8de-4f1c-b965-d41258e2373c";
    UUID uuid = java.util.UUID.fromString(uuidStr);
    String byteStr = "0xd79c8651b8de4f1cb965d41258e2373c";
    encodeAndDecode(uuidCodec, uuid, byteStr);
    encodeAndDecode(uuidCodec, null, null);

    encodeObjectAndDecode(uuidCodec, uuidStr, byteStr, uuid);
    encodeObjectAndDecode(uuidCodec, uuid, byteStr, uuid);
    encodeObjectAndDecode(uuidCodec, null, null, null);
    encodeObjectAndDecode(uuidCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleVarchar() {
    Codec<String> varcharCodec = mapper.codecFor(primitive(VARCHAR));
    assertThat(varcharCodec).isSameAs(mapper.varchar);

    encodeAndDecode(varcharCodec, "hello", "0x68656c6c6f");
    encodeAndDecode(varcharCodec, "", "0x");
    encodeAndDecode(varcharCodec, "∆yøπ", "0xe2888679c3b8cf80");
    encodeAndDecode(varcharCodec, null, null);

    encodeObjectAndDecode(varcharCodec, "hello", "0x68656c6c6f", "hello");
    encodeObjectAndDecode(varcharCodec, 5, "0x35", "5");
    encodeObjectAndDecode(varcharCodec, null, null, null);
  }

  @Test
  public void shouldHandleVarint() {
    Codec<BigInteger> varintCodec = mapper.codecFor(primitive(VARINT));
    assertThat(varintCodec).isSameAs(mapper.varint);

    BigInteger bigInt = new BigInteger("8574747744773434", 10);
    String bigIntStr = "0x1e76b0095da53a";

    encodeAndDecode(varintCodec, BigInteger.ZERO, "0x00");
    encodeAndDecode(varintCodec, bigInt, bigIntStr);
    encodeAndDecode(varintCodec, null, null);

    encodeObjectAndDecode(varintCodec, bigInt, bigIntStr, bigInt);
    encodeObjectAndDecode(varintCodec, "8574747744773434", bigIntStr, bigInt);
    encodeObjectAndDecode(varintCodec, 8577, "0x2181", BigInteger.valueOf(8577));
    encodeObjectAndDecode(varintCodec, null, null, null);
    encodeObjectAndDecode(varintCodec, Optional.of(12), null, null);
  }

  @Test
  public void shouldHandleList() {
    Codec<List<String>> listStringCodec = mapper.codecFor(new RawList(primitive(VARCHAR)));
    Codec<List<String>> listStringCodec2 = mapper.codecFor(new RawList(primitive(VARCHAR)));
    // Repetitive calls should yield thes ame instance.
    assertThat(listStringCodec).isSameAs(listStringCodec2);

    List<String> l = new ArrayList<>();
    l.add("one");
    l.add("two");
    l.add("one");
    String listStr = "0x00000003000000036f6e650000000374776f000000036f6e65";

    encodeAndDecode(
        listStringCodec, Collections.singletonList("hello"), "0x000000010000000568656c6c6f");
    encodeAndDecode(listStringCodec, l, listStr);
    encodeAndDecode(listStringCodec, Collections.emptyList(), "0x00000000");
    encodeAndDecode(listStringCodec, null, null, Collections.emptyList());

    encodeObjectAndDecode(listStringCodec, l, listStr, l);
    encodeObjectAndDecode(listStringCodec, new LinkedBlockingQueue<>(l), listStr, l);
    encodeObjectAndDecode(listStringCodec, null, null, Collections.emptyList());
    encodeObjectAndDecode(listStringCodec, Optional.of(12), null, Collections.emptyList());

    // Null value in List
    try {
      encodeAndDecode(listStringCodec, Collections.singletonList(null), null);
      fail("Expected NPE");
    } catch (NullPointerException e) { // expected
    }

    // List not matching type for Codec
    try {
      List intList = new ArrayList<>();
      intList.add(1);
      encodeAndDecode(listStringCodec, intList, "");
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      assertThat(e.getCause()).isInstanceOf(ClassCastException.class);
    }

    // Underflow while decoding
    try {
      ByteBuffer buf = listStringCodec.encode(l);
      // half limit so buffer is truncated.
      buf.limit(buf.limit() / 2);
      listStringCodec.decode(buf);
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      assertThat(e.getCause()).isInstanceOf(BufferUnderflowException.class);
    }

    // Values of set should be mapped using encodeObject
    // In this case we have a codec that calls for list<ascii> but we give it a Set<Integer>.
    // The codec should properly convert to List<String> from List<Integer>.
    Set<Integer> intSet = new LinkedHashSet<>();
    intSet.add(1);
    intSet.add(2);
    intSet.add(-40);

    List<String> stringList = new ArrayList<>();
    stringList.add("1");
    stringList.add("2");
    stringList.add("-40");
    encodeObjectAndDecode(
        listStringCodec, intSet, "0x0000000300000001310000000132000000032d3430", stringList);
  }

  @Test
  public void shouldHandleSet() {
    Codec<Set<Integer>> setIntCodec = mapper.codecFor(new RawSet(primitive(INT)));
    Codec<Set<Integer>> setIntCodec2 = mapper.codecFor(new RawSet(primitive(INT)));
    assertThat(setIntCodec).isSameAs(setIntCodec2);

    Set<Integer> s = new LinkedHashSet<>();
    s.add(1);
    s.add(2);
    s.add(-40);
    String setStr = "0x000000030000000400000001000000040000000200000004ffffffd8";

    encodeAndDecode(setIntCodec, Collections.singleton(77), "0x00000001000000040000004d");
    encodeAndDecode(setIntCodec, s, setStr);
    encodeAndDecode(setIntCodec, Collections.emptySet(), "0x00000000");
    encodeAndDecode(setIntCodec, null, null, Collections.emptySet());

    encodeObjectAndDecode(setIntCodec, s, setStr, s);
    encodeObjectAndDecode(setIntCodec, new ArrayList<>(s), setStr, s);
    encodeObjectAndDecode(setIntCodec, null, null, Collections.emptySet());
    encodeObjectAndDecode(setIntCodec, Optional.of(12), null, Collections.emptySet());

    // Null value in Set
    try {
      encodeAndDecode(setIntCodec, Collections.singleton(null), null);
      fail("Expected NPE");
    } catch (NullPointerException e) { // expected
    }

    // Set not matching type for Codec
    try {
      Set stringSet = new LinkedHashSet<>();
      stringSet.add("hello");
      encodeAndDecode(setIntCodec, stringSet, "");
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      assertThat(e.getCause()).isInstanceOf(ClassCastException.class);
    }

    // Underflow while decoding
    try {
      ByteBuffer buf = setIntCodec.encode(s);
      // half limit so buffer is truncated.
      buf.limit(buf.limit() / 2);
      setIntCodec.decode(buf);
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      assertThat(e.getCause()).isInstanceOf(BufferUnderflowException.class);
    }

    // Values of set should be mapped using encodeObject
    // In this case we have a codec that calls for set<int> but we give it a Set<String>.
    // The codec should properly convert to Set<Integer> from Set<String>.
    Set<String> stringSet = new LinkedHashSet<>();
    stringSet.add("1");
    stringSet.add("2");
    stringSet.add("-40");
    encodeObjectAndDecode(setIntCodec, stringSet, setStr, s);
  }

  @Test
  @SuppressWarnings("Unchecked")
  public void shouldHandleMap() {
    Codec<Map<String, Integer>> mapCodec =
        mapper.codecFor(new RawType.RawMap(primitive(ASCII), primitive(INT)));
    Codec<Map<String, Integer>> mapCodec2 =
        mapper.codecFor(new RawType.RawMap(primitive(ASCII), primitive(INT)));
    assertThat(mapCodec).isSameAs(mapCodec2);

    Map<String, Integer> m = new LinkedHashMap<>();
    m.put("hello", 8675);
    m.put("world", 309);
    String mapStr =
        "0x000000020000000568656c6c6f00000004000021e300000005776f726c640000000400000135";

    encodeAndDecode(mapCodec, m, mapStr);
    encodeAndDecode(mapCodec, null, null, Collections.emptyMap());

    encodeObjectAndDecode(mapCodec, m, m);
    encodeObjectAndDecode(mapCodec, null, null, Collections.emptyMap());
    encodeObjectAndDecode(mapCodec, Optional.of(12), null, Collections.emptyMap());

    // Null values not allowed.
    try {
      Map<String, Integer> mWithNull = new LinkedHashMap<>();
      mWithNull.put("Hello", null);
      encodeAndDecode(mapCodec, mWithNull, "");
      fail("Expected NPE");
    } catch (NullPointerException e) {
      // expected
    }

    // Map not matching codec for key.
    try {
      Map mIntKey = new LinkedHashMap<>();
      mIntKey.put(5, 6);
      encodeAndDecode(mapCodec, mIntKey, "");
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      // expected
      assertThat(e.getCause()).isInstanceOf(ClassCastException.class);
    }

    // Map not matching codec for value.
    try {
      Map mStrVal = new LinkedHashMap<>();
      mStrVal.put("hello", "world");
      encodeAndDecode(mapCodec, mStrVal, "");
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) {
      // expected
      assertThat(e.getCause()).isInstanceOf(ClassCastException.class);
    }

    // Underflow while decoding
    try {
      ByteBuffer buf = mapCodec.encode(m);
      // half limit so buffer is truncated.
      buf.limit(buf.limit() / 2);
      mapCodec.decode(buf);
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) { // expected
    }

    // Values of map should be mapped using encodeObject
    // In this case we have a codec that calls for map<ascii, int> but we give it a Map<Integer,
    // String>
    // The codec should properly convert to Map<String, Integer> when encoding.
    Map<Integer, String> swapMap1 = new HashMap<>();
    swapMap1.put(0, "1");
    swapMap1.put(1, "0");
    Map<String, Integer> swapMap0 = new HashMap<>();
    swapMap0.put("0", 1);
    swapMap0.put("1", 0);

    encodeObjectAndDecode(mapCodec, swapMap1, swapMap0);
  }

  @Test
  public void shouldHandleTuple() {
    List<RawType> rawTypes = new ArrayList<>();
    rawTypes.add(primitive(ASCII));
    rawTypes.add(primitive(INT));
    rawTypes.add(primitive(DOUBLE));
    RawTuple tupleType = new RawTuple(rawTypes);
    Tuple t = new Tuple(tupleType, "hello", -42, 3.1417);
    Codec<Tuple> tupleCodec = mapper.codecFor(tupleType);

    // ensure caching works.
    assertThat(mapper.codecFor(tupleType)).isSameAs(tupleCodec);

    String expectedByteStr = "0x0000000568656c6c6f00000004ffffffd600000008400922339c0ebee0";
    encodeAndDecode(tupleCodec, t, expectedByteStr);
    encodeAndDecode(tupleCodec, null, null, null);

    Tuple tWithNull = new Tuple(tupleType, null, -42, 3.1417);
    encodeAndDecode(tupleCodec, tWithNull, "0xffffffff00000004ffffffd600000008400922339c0ebee0");

    encodeObjectAndDecode(tupleCodec, t, expectedByteStr, t);

    // should work for list and coerce.
    List<Object> l = new ArrayList<>();
    l.add("hello");
    l.add("-42");
    l.add(3.1417);
    encodeObjectAndDecode(tupleCodec, l, expectedByteStr, t);
    encodeObjectAndDecode(tupleCodec, 1, null, null); // non-matching type
    encodeObjectAndDecode(tupleCodec, Collections.singleton("HI"), null, null); // mismatch size

    RawTuple tupleTypeSingle = new RawTuple(Collections.singletonList(primitive(ASCII)));
    Tuple tSingle = new Tuple(tupleTypeSingle, "HI");
    encodeObjectAndDecode(tupleCodec, tSingle, null, null); // mismatch tuple

    // Underflow while decoding
    try {
      ByteBuffer buf = tupleCodec.encode(t);
      // half limit so buffer is truncated.
      buf.limit(buf.limit() / 2);
      tupleCodec.decode(buf);
      fail("Expected InvalidTypeException");
    } catch (InvalidTypeException e) { // expected
    }
  }

  @Test
  public void shouldHandleNestedCollection() {
    Codec<Set<List<Double>>> setListDoubleCodec =
        mapper.codecFor(new RawSet(new RawList(primitive(DOUBLE))));
    Codec<Set<List<Double>>> setListDoubleCodec2 =
        mapper.codecFor(new RawSet(new RawList(primitive(DOUBLE))));
    assertThat(setListDoubleCodec).isSameAs(setListDoubleCodec2);

    Set<List<Double>> s = new LinkedHashSet<>();
    List<Double> l0 = Collections.singletonList(8675.09);
    List<Double> l1 = new ArrayList<>();
    l1.add(86.75);
    l1.add(30.9);
    s.add(l0);
    s.add(l1);

    encodeAndDecode(
        setListDoubleCodec,
        s,
        "0x0000000200000010000000010000000840c0f18b851eb8520000001c00000002000000084055b0000000000000000008403ee66666666666");
  }

  @Test
  public void shouldNotAllowProtocolV4CodecsToBeUsedForProtocolV3() {
    List<RawType> rawTypes = new ArrayList<>();
    rawTypes.add(primitive(TINYINT));
    rawTypes.add(primitive(SMALLINT));
    rawTypes.add(primitive(DATE));
    rawTypes.add(primitive(TIME));

    CqlMapper mapper = CqlMapper.forVersion(3);
    for (RawType rawType : rawTypes) {
      Codec<?> codec = mapper.codecFor(rawType);
      try {
        codec.decode(null);
        fail("Shouldn't have been able to decode " + rawType);
      } catch (ProtocolVersionException e) {
        assertThat(e.getMinVersion()).isEqualTo(4);
        assertThat(e.getVersion()).isEqualTo(3);
        assertThat(e.getType()).isEqualTo(rawType);
      }
      try {
        codec.encode(null);
        fail("Shouldn't have been able to encode " + rawType);
      } catch (ProtocolVersionException e) {
        assertThat(e.getMinVersion()).isEqualTo(4);
        assertThat(e.getVersion()).isEqualTo(3);
        assertThat(e.getType()).isEqualTo(rawType);
      }
    }
  }

  <T> void encodeAndDecode(Codec<T> codec, T input, String expectedByteStr) {
    encodeAndDecode(codec, input, expectedByteStr, input);
  }

  <T> void encodeAndDecode(Codec<T> codec, T input, String expectedByteStr, T expected) {
    ByteBuffer byteBuf = codec.encode(input);
    assertThat(byteBuf).hasBytes(expectedByteStr);
    assertThat(codec.decode(byteBuf)).isEqualTo(expected);
  }

  <T> void encodeObjectAndDecode(Codec<T> codec, Object input, String expectedByteStr, T expected) {
    ByteBuffer byteBuf = codec.encodeObject(input);
    assertThat(byteBuf).hasBytes(expectedByteStr);
    assertThat(codec.decode(byteBuf)).isEqualTo(expected);
  }

  <T> void encodeObjectAndDecode(Codec<T> codec, Object input, T expected) {
    ByteBuffer byteBuf = codec.encodeObject(input);
    assertThat(codec.decode(byteBuf)).isEqualTo(expected);
  }
}
