package com.datastax.simulacron.common.codec;

import org.junit.Test;

import java.nio.ByteBuffer;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.ASCII;
import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.VARCHAR;
import static com.datastax.simulacron.common.Assertions.assertThat;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;

public class CqlMapperTest {

  private CqlMapper mapper = CqlMapper.forVersion(4);

  @Test
  public void shouldHandleAscii() {
    Codec<String> asciiCodec = mapper.codecFor(primitive(ASCII));
    assertThat(asciiCodec).isEqualTo(mapper.ascii);

    encodeAndDecode(asciiCodec, "hello", "0x68656c6c6f");
    encodeAndDecode(asciiCodec, "", "0x");
  }

  @Test
  public void shouldHandleVarchar() {
    Codec<String> varcharCodec = mapper.codecFor(primitive(VARCHAR));
    assertThat(varcharCodec).isEqualTo(mapper.varchar);

    encodeAndDecode(varcharCodec, "hello", "0x68656c6c6f");
    encodeAndDecode(varcharCodec, "", "0x");
    encodeAndDecode(varcharCodec, "∆yøπ", "0xe2888679c3b8cf80");
  }

  <T> void encodeAndDecode(Codec<T> codec, T input, String expectedByteStr) {
    ByteBuffer byteBuf = codec.encode(input);
    assertThat(byteBuf).hasBytes(expectedByteStr);
    assertThat(codec.decode(byteBuf)).isEqualTo(input);
  }
}
