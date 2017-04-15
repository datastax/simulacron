package com.datastax.simulacron.common.codec;

import com.datastax.oss.protocol.internal.response.result.RawType;

public class ProtocolVersionException extends InvalidTypeException {

  private final RawType rawType;
  private final int version;
  private final int minVersion;

  public ProtocolVersionException(RawType rawType, int version, int minVersion) {
    super(rawType + " requires protocol version " + minVersion + " but " + version + " is in use");
    this.rawType = rawType;
    this.version = version;
    this.minVersion = minVersion;
  }

  public RawType getType() {
    return rawType;
  }

  public int getVersion() {
    return version;
  }

  public int getMinVersion() {
    return minVersion;
  }
}
