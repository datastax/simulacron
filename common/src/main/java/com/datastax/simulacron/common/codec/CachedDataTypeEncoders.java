package com.datastax.simulacron.common.codec;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class CachedDataTypeEncoders implements DataTypeEncoders {

  private final int protocolVersion;

  public CachedDataTypeEncoders(int protocolVersion) {
    this.protocolVersion = protocolVersion;
  }

  @Override
  public int protocolVersion() {
    return protocolVersion;
  }

  private final ConcurrentMap<Encoder<?>, Encoder<?>> collectionEncoders =
      new ConcurrentHashMap<>();

  @Override
  @SuppressWarnings("unchecked")
  public <E, C extends Collection<E>> Encoder<C> collectionOf(Encoder<E> encoder) {
    Encoder<?> collectionEncoder = collectionEncoders.get(encoder);
    if (collectionEncoder == null) {
      Encoder<C> newEncoder =
          (C v) -> {
            ByteBuffer[] bbs = new ByteBuffer[v.size()];
            int i = 0;
            for (E elt : v) {
              if (elt == null) {
                throw new NullPointerException("Collection elements cannot be null");
              }
              ByteBuffer bb;
              try {
                bb = encoder.apply(elt);
              } catch (ClassCastException e) {
                throw new RuntimeException("Invalid type for element");
              }
              bbs[i++] = bb;
            }
            return pack(bbs);
          };

      collectionEncoder = collectionEncoders.computeIfAbsent(encoder, k -> newEncoder);
    }

    return (Encoder<C>) collectionEncoder;
  }

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
    switch (protocolVersion()) {
      case 1:
      case 2:
        int elemSize = value.remaining();
        if (elemSize > 65535)
          throw new IllegalArgumentException(
              String.format(
                  "Native protocol version %d supports only elements with size up to 65535 bytes - but element size is %d bytes",
                  protocolVersion(), elemSize));
        return 2 + elemSize;
      case 3:
      case 4:
      case 5:
      case 65:
        return value == null ? 4 : 4 + value.remaining();
      default:
        throw new IllegalArgumentException(
            "Protocol version " + protocolVersion() + " is not supported.");
    }
  }

  private int sizeOfCollectionSize() {
    switch (protocolVersion()) {
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
            "Protocol version " + protocolVersion() + " is not supported.");
    }
  }

  private void writeSize(ByteBuffer output, int size) {
    switch (protocolVersion()) {
      case 1:
      case 2:
        if (size > 65535)
          throw new IllegalArgumentException(
              String.format(
                  "Native protocol version %d supports up to 65535 elements in any collection - but collection contains %d elements",
                  protocolVersion(), size));
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
            "Protocol version " + protocolVersion() + " is not supported.");
    }
  }

  private void writeValue(ByteBuffer output, ByteBuffer value) {
    switch (protocolVersion()) {
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
            "Protocol version " + protocolVersion() + " is not supported.");
    }
  }
}
