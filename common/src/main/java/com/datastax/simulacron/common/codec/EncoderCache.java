package com.datastax.simulacron.common.codec;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class EncoderCache {

  private static final ConcurrentMap<Integer, DataTypeEncoders> encoderCache =
      new ConcurrentHashMap<>();

  public static DataTypeEncoders getEncoders(int protocolVersion) {
    return encoderCache.computeIfAbsent(protocolVersion, CachedDataTypeEncoders::new);
  }
}
