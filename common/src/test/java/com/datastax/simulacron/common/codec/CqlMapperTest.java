package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RawType.RawList;
import com.datastax.oss.protocol.internal.response.result.RawType.RawSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.*;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.*;
import static com.datastax.simulacron.common.Assertions.assertThat;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;
import static org.assertj.core.api.Assertions.fail;

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
  }

  @Test
  public void shouldHandleBoolean() {
    Codec<Boolean> booleanCodec = mapper.codecFor(primitive(BOOLEAN));
    assertThat(booleanCodec).isSameAs(mapper.bool);

    encodeAndDecode(booleanCodec, true, "0x01");
    encodeAndDecode(booleanCodec, false, "0x00");
    encodeAndDecode(booleanCodec, null, null, false);

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
  }

  @Test
  public void shouldHandleDecimal() {
    Codec<BigDecimal> decimalCodec = mapper.codecFor(primitive(DECIMAL));
    assertThat(decimalCodec).isSameAs(mapper.decimal);

    encodeAndDecode(
        decimalCodec, new BigDecimal(8675e39), "0x00000000639588a2c184700000000000000000000000");
    encodeAndDecode(decimalCodec, new BigDecimal(0), "0x0000000000");
    encodeAndDecode(decimalCodec, null, null);

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

    // ipv4
    encodeAndDecode(inetCodec, InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), "0x7f000001");
    // ipv6
    encodeAndDecode(
        inetCodec,
        InetAddress.getByAddress(
            new byte[] {127, 0, 0, 1, 127, 0, 0, 2, 127, 0, 0, 3, 122, 127, 125, 124}),
        "0x7f0000017f0000027f0000037a7f7d7c");
    encodeAndDecode(inetCodec, null, null);

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
  }

  @Test
  public void shouldHandleTimestamp() throws Exception {
    Codec<Date> timestampCodec = mapper.codecFor(primitive(TIMESTAMP));
    assertThat(timestampCodec).isSameAs(mapper.timestamp);

    encodeAndDecode(timestampCodec, new Date(0), "0x0000000000000000");
    encodeAndDecode(timestampCodec, null, null);
  }

  @Test
  public void shouldHandleTimeuuid() throws Exception {
    Codec<UUID> timeuuidCodec = mapper.codecFor(primitive(TIMEUUID));
    assertThat(timeuuidCodec).isSameAs(mapper.timeuuid);

    encodeAndDecode(
        timeuuidCodec,
        java.util.UUID.fromString("5bc64000-2157-11e7-bf6d-934d002a66ff"),
        "0x5bc64000215711e7bf6d934d002a66ff");
    encodeAndDecode(timeuuidCodec, null, null);
  }

  @Test
  public void shouldHandleTinyint() throws Exception {
    Codec<Byte> tinyintCodec = mapper.codecFor(primitive(TINYINT));
    assertThat(tinyintCodec).isSameAs(mapper.tinyint);

    encodeAndDecode(tinyintCodec, (byte) 0, "0x00");
    encodeAndDecode(tinyintCodec, Byte.MAX_VALUE, "0x7f");
    encodeAndDecode(tinyintCodec, Byte.MIN_VALUE, "0x80");
    encodeAndDecode(tinyintCodec, null, null, (byte) 0);

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

    encodeAndDecode(
        uuidCodec,
        java.util.UUID.fromString("d79c8651-b8de-4f1c-b965-d41258e2373c"),
        "0xd79c8651b8de4f1cb965d41258e2373c");
    encodeAndDecode(uuidCodec, null, null);
  }

  @Test
  public void shouldHandleVarchar() {
    Codec<String> varcharCodec = mapper.codecFor(primitive(VARCHAR));
    assertThat(varcharCodec).isSameAs(mapper.varchar);

    encodeAndDecode(varcharCodec, "hello", "0x68656c6c6f");
    encodeAndDecode(varcharCodec, "", "0x");
    encodeAndDecode(varcharCodec, "∆yøπ", "0xe2888679c3b8cf80");
    encodeAndDecode(varcharCodec, null, null);
  }

  @Test
  public void shouldHandleVarint() {
    Codec<BigInteger> varintCodec = mapper.codecFor(primitive(VARINT));
    assertThat(varintCodec).isSameAs(mapper.varint);

    encodeAndDecode(varintCodec, BigInteger.ZERO, "0x00");
    encodeAndDecode(varintCodec, new BigInteger("8574747744773434", 10), "0x1e76b0095da53a");
    encodeAndDecode(varintCodec, null, null);
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

    encodeAndDecode(
        listStringCodec, Collections.singletonList("hello"), "0x000000010000000568656c6c6f");
    encodeAndDecode(listStringCodec, l, "0x00000003000000036f6e650000000374776f000000036f6e65");
    encodeAndDecode(listStringCodec, Collections.emptyList(), "0x00000000");
    encodeAndDecode(listStringCodec, null, null, Collections.emptyList());

    try {
      encodeAndDecode(listStringCodec, Collections.singletonList(null), null);
    } catch (NullPointerException e) { // expected
    }
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

    encodeAndDecode(setIntCodec, Collections.singleton(77), "0x00000001000000040000004d");
    encodeAndDecode(setIntCodec, s, "0x000000030000000400000001000000040000000200000004ffffffd8");
    encodeAndDecode(setIntCodec, Collections.emptySet(), "0x00000000");
    encodeAndDecode(setIntCodec, null, null, Collections.emptySet());

    try {
      encodeAndDecode(setIntCodec, Collections.singleton(null), null);
    } catch (NullPointerException e) { // expected
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
}
