package com.datastax.simulacron.common.codec;

public enum ConsistencyLevel {
  ANY(0),
  ONE(1),
  TWO(2),
  THREE(3),
  QUORUM(4),
  ALL(5),
  LOCAL_QUORUM(6),
  EACH_QUORUM(7),
  SERIAL(8),
  LOCAL_SERIAL(9),
  LOCAL_ONE(10);

  final int code;

  private ConsistencyLevel(int code) {
    this.code = code;
  }

  public static ConsistencyLevel fromString(String text) {
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      if (cl.name().equalsIgnoreCase(text)) {
        return cl;
      }
    }
    return null;
  }

  public static ConsistencyLevel fromCode(int code) {
    for (ConsistencyLevel cl : ConsistencyLevel.values()) {
      if (cl.code == code) {
        return cl;
      }
    }
    return null;
  }
}
