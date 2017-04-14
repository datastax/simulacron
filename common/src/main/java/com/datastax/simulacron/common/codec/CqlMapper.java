package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.response.result.RawType.RawList;
import com.datastax.oss.protocol.internal.response.result.RawType.RawSet;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.UUID;

import static com.datastax.oss.protocol.internal.ProtocolConstants.DataType.*;
import static com.datastax.simulacron.common.codec.CodecUtils.primitive;

public class CqlMapper {

  private static final Logger logger = LoggerFactory.getLogger(CqlMapper.class);

  private final int protocolVersion;

  // TODO: Perhaps make these sizebound.
  // TODO: Follow optimizations that java driver makes in codec registry.
  // For resolving by cqlType.
  private final LoadingCache<RawType, Codec<?>> cqlTypeCache =
      Caffeine.newBuilder().build(this::loadCache);

  private final TypeFactory typeFactory = TypeFactory.defaultInstance();

  private static final LoadingCache<Integer, CqlMapper> mapperCache =
      Caffeine.newBuilder().build(CqlMapper::new);

  public static CqlMapper forVersion(int protocolVersion) {
    return mapperCache.get(protocolVersion);
  }

  CqlMapper(int protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  private <T> void register(Codec<T> codec) {
    cqlTypeCache.put(codec.getCqlType(), codec);
  }

  @SuppressWarnings("unchecked")
  private Codec<?> loadCache(RawType key) {
    if (key instanceof RawSet) {
      RawSet s = (RawSet) key;
      Codec<?> elementCodec = cqlTypeCache.get(s.elementType);
      return new SetCodec(elementCodec);
    } else if (key instanceof RawList) {
      RawList l = (RawList) key;
      Codec<?> elementCodec = cqlTypeCache.get(l.elementType);
      return new ListCodec(elementCodec);
    } else {
      // TODO support other collection(ish) codecs, Tuple, Map, UDT.
      logger.warn("Could not resolve codec for {}, returning null instead.", key);
      return null;
    }
  }

  @SuppressWarnings("unchecked")
  public <T> Codec<T> codecFor(RawType cqlType) {
    return (Codec<T>) cqlTypeCache.get(cqlType);
  }

  abstract class AbstractCodec<T> implements Codec<T> {

    private final JavaType javaType;
    private final RawType cqlType;

    AbstractCodec(JavaType javaType, RawType cqlType) {
      this(javaType, cqlType, true);
    }

    AbstractCodec(JavaType javaType, RawType cqlType, boolean register) {
      this.javaType = javaType;
      this.cqlType = cqlType;
      if (register) {
        register(this);
      }
    }

    @Override
    public JavaType getJavaType() {
      return javaType;
    }

    @Override
    public RawType getCqlType() {
      return cqlType;
    }
  }

  class StringCodec extends AbstractCodec<String> {
    private final String charset;

    StringCodec(String charset, RawType cqlType) {
      super(typeFactory.constructType(String.class), cqlType);
      this.charset = charset;
    }

    @Override
    public ByteBuffer encode(String input) {
      try {
        return ByteBuffer.wrap(input.getBytes(charset));
      } catch (UnsupportedEncodingException uee) {
        throw new RuntimeException("Invalid value " + input);
      }
    }

    @Override
    public String decode(ByteBuffer input) {
      if (input == null) {
        return null;
      } else if (input.remaining() == 0) {
        return "";
      } else {
        try {
          return new String(Bytes.getArray(input), charset);
        } catch (UnsupportedEncodingException e) {
          throw new RuntimeException("Could not extract bytes", e);
        }
      }
    }
  }

  abstract class CollectionCodec<E, C extends Collection<E>> extends AbstractCodec<C> {

    private final Codec<E> elementCodec;

    CollectionCodec(JavaType javaType, RawType cqlType, Codec<E> elementCodec) {
      super(javaType, cqlType, false);
      this.elementCodec = elementCodec;
    }

    @Override
    public ByteBuffer encode(C input) {
      ByteBuffer[] bbs = new ByteBuffer[input.size()];
      int i = 0;
      for (E elt : input) {
        if (elt == null) {
          throw new NullPointerException("Collection elements cannot be null");
        }
        ByteBuffer bb;
        try {
          bb = elementCodec.encode(elt);
        } catch (ClassCastException e) {
          throw new RuntimeException("Invalid type for element");
        }
        bbs[i++] = bb;
      }
      return pack(bbs);
    }

    @Override
    public C decode(ByteBuffer input) {
      if (input == null || input.remaining() == 0) return newInstance(0);
      try {
        ByteBuffer i = input.duplicate();
        int size = readSize(i);
        C coll = newInstance(size);
        for (int pos = 0; pos < size; pos++) {
          ByteBuffer databb = readValue(i);
          coll.add(elementCodec.decode(databb));
        }
        return coll;
      } catch (BufferUnderflowException e) {
        throw new RuntimeException("Not enough bytes to deserialize collection", e);
      }
    }

    protected abstract C newInstance(int size);
  }

  class SetCodec<E> extends CollectionCodec<E, Set<E>> {

    SetCodec(Codec<E> elementCodec) {
      super(
          typeFactory.constructCollectionLikeType(Set.class, elementCodec.getJavaType()),
          new RawSet(elementCodec.getCqlType()),
          elementCodec);
    }

    @Override
    protected Set<E> newInstance(int size) {
      return new LinkedHashSet<>(size);
    }
  }

  class ListCodec<E> extends CollectionCodec<E, List<E>> {

    ListCodec(Codec<E> elementCodec) {
      super(
          typeFactory.constructCollectionLikeType(List.class, elementCodec.getJavaType()),
          new RawList(elementCodec.getCqlType()),
          elementCodec);
    }

    @Override
    protected List<E> newInstance(int size) {
      return new ArrayList<>(size);
    }
  }

  public final Codec<String> ascii = new StringCodec("US-ASCII", primitive(ASCII));

  public final Codec<String> varchar = new StringCodec("UTF-8", primitive(VARCHAR));

  public Codec<InetAddress> inet =
      new AbstractCodec<InetAddress>(
          typeFactory.constructType(InetAddress.class), primitive(INET)) {

        @Override
        public ByteBuffer encode(InetAddress input) {
          return ByteBuffer.wrap(input.getAddress());
        }

        @Override
        public InetAddress decode(ByteBuffer input) {
          if (input == null || input.remaining() == 0) return null;
          try {
            return InetAddress.getByAddress(Bytes.getArray(input));
          } catch (UnknownHostException e) {
            throw new RuntimeException(
                "Invalid bytes for inet value, got " + input.remaining() + " bytes");
          }
        }
      };

  public Codec<UUID> uuid =
      new AbstractCodec<UUID>(typeFactory.constructType(UUID.class), primitive(DataType.UUID)) {

        @Override
        public ByteBuffer encode(UUID input) {
          ByteBuffer buf = ByteBuffer.allocate(16);
          buf.putLong(0, input.getMostSignificantBits());
          buf.putLong(8, input.getLeastSignificantBits());
          return buf;
        }

        @Override
        public UUID decode(ByteBuffer input) {
          return input == null || input.remaining() == 0
              ? null
              : new UUID(input.getLong(input.position()), input.getLong(input.position() + 8));
        }
      };

  private ByteBuffer pack(ByteBuffer[] buffers) {
    int size = 0;
    for (ByteBuffer bb : buffers) {
      int elemSize = sizeOfValue(bb);
      size += elemSize;
    }
    ByteBuffer result = ByteBuffer.allocate(sizeOfCollectionSize() + size);
    writeSize(result, buffers.length);
    for (ByteBuffer bb : buffers) {
      writeValue(result, bb);
    }
    return (ByteBuffer) result.flip();
  }

  private int sizeOfValue(ByteBuffer value) {
    switch (protocolVersion) {
      case 1:
      case 2:
        int elemSize = value.remaining();
        if (elemSize > 65535)
          throw new IllegalArgumentException(
              String.format(
                  "Native protocol version %d supports only elements with size up to 65535 bytes - but element size is %d bytes",
                  protocolVersion, elemSize));
        return 2 + elemSize;
      case 3:
      case 4:
      case 5:
      case 65:
        return value == null ? 4 : 4 + value.remaining();
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion + " is not supported.");
    }
  }

  private int sizeOfCollectionSize() {
    switch (protocolVersion) {
      case 1:
      case 2:
        return 2;
      case 3:
      case 4:
      case 5:
      case 65:
        return 4;
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion + " is not supported.");
    }
  }

  private int readSize(ByteBuffer input) {
    switch (protocolVersion) {
      case 1:
      case 2:
        return getUnsignedShort(input);
      case 3:
      case 4:
      case 5:
      case 65:
        return input.getInt();
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion + " is not supported.");
    }
  }

  private static int getUnsignedShort(ByteBuffer bb) {
    int length = (bb.get() & 0xFF) << 8;
    return length | (bb.get() & 0xFF);
  }

  private void writeSize(ByteBuffer output, int size) {
    switch (protocolVersion) {
      case 1:
      case 2:
        if (size > 65535)
          throw new IllegalArgumentException(
              String.format(
                  "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                  protocolVersion, size));
        output.putShort((short) size);
        break;
      case 3:
      case 4:
      case 5:
      case 65:
        output.putInt(size);
        break;
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion + " is not supported.");
    }
  }

  private ByteBuffer readValue(ByteBuffer input) {
    int size = readSize(input);
    return size < 0 ? null : readBytes(input, size);
  }

  private ByteBuffer readBytes(ByteBuffer bb, int length) {
    ByteBuffer copy = bb.duplicate();
    copy.limit(copy.position() + length);
    bb.position(bb.position() + length);
    return copy;
  }

  private void writeValue(ByteBuffer output, ByteBuffer value) {
    switch (protocolVersion) {
      case 1:
      case 2:
        assert value != null;
        output.putShort((short) value.remaining());
        output.put(value.duplicate());
        break;
      case 3:
      case 4:
      case 5:
      case 65:
        if (value == null) {
          output.putInt(-1);
        } else {
          output.putInt(value.remaining());
          output.put(value.duplicate());
        }
        break;
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion + " is not supported.");
    }
  }
}
